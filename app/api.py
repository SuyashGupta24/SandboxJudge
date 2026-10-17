"""
Phase 4: Flask REST API.

  POST /api/v1/submit            202 Accepted + submission_id   (non-blocking)
  GET  /api/v1/status/<id>       current state + metrics        (poll this)
  GET  /api/v1/problems          list problems
  GET  /api/v1/problems/<id>     problem detail + one sample
  GET  /api/v1/health            queue / worker stats
  GET  /                         minimal UI
"""
from __future__ import annotations

import logging
import queue
import uuid
from typing import Optional

from flask import Blueprint, Flask, jsonify, render_template, request
from sqlalchemy import select
from werkzeug.middleware.proxy_fix import ProxyFix

from app.config import APP
from app.middleware import (
    RateLimiter,
    rate_limited,
    register_error_handlers,
    validate_submit_payload,
)
from app.models import Problem, Submission, session_scope
from app.worker import PoolClosed, WorkerPool

logger = logging.getLogger(__name__)


def create_app(
    pool: WorkerPool,
    *,
    submit_limit: Optional[int] = None,
    status_limit: Optional[int] = None,
) -> Flask:
    app = Flask(__name__)  # templates live in app/templates
    app.config["MAX_CONTENT_LENGTH"] = APP.max_source_bytes + APP.max_stdin_bytes + 4096
    if APP.trust_proxy:  # only behind a reverse proxy YOU control
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1)  # type: ignore[method-assign]
    register_error_handlers(app)

    submit_rl = RateLimiter(submit_limit or APP.submit_rate_limit, APP.rate_window_s)
    status_rl = RateLimiter(status_limit or APP.status_rate_limit, APP.rate_window_s)
    api = Blueprint("api", __name__, url_prefix="/api/v1")

    # ------------------------------------------------------------- submit
    @api.post("/submit")
    @rate_limited(submit_rl)
    def submit():
        req = validate_submit_payload(request.get_json(silent=True))

        with session_scope() as session:
            total_cases = 1
            if req.problem_id is not None:
                problem = session.get(Problem, req.problem_id)
                if problem is None:
                    return jsonify(error=f"Problem {req.problem_id} not found."), 404
                total_cases = len(problem.test_cases)
            sub = Submission(
                language=req.language,
                source_code=req.source_code,
                problem_id=req.problem_id,
                custom_stdin=req.stdin,
                total_testcases=total_cases,
            )
            session.add(sub)
            session.flush()
            submission_id = sub.id
        # Row is committed (PENDING) BEFORE it is queued, so a worker can never see a ghost id.

        try:
            pool.enqueue(submission_id)
        except (queue.Full, PoolClosed) as exc:
            with session_scope() as session:  # nobody has seen this id yet; remove it
                row = session.get(Submission, submission_id)
                if row is not None:
                    session.delete(row)
            resp = jsonify(error="Judge is busy, try again shortly.")
            resp.status_code = 503
            resp.headers["Retry-After"] = "5"
            logger.warning("rejected submission: %s", type(exc).__name__)
            return resp

        return (
            jsonify(
                submission_id=submission_id,
                status="PENDING",
                status_url=f"/api/v1/status/{submission_id}",
            ),
            202,
        )

    # ------------------------------------------------------------- status
    @api.get("/status/<submission_id>")
    @rate_limited(status_rl)
    def status(submission_id: str):
        try:
            uuid.UUID(submission_id)
        except ValueError:
            return jsonify(error="Malformed submission id."), 400
        with session_scope() as session:
            sub = session.get(Submission, submission_id)
            if sub is None:
                return jsonify(error="Submission not found."), 404
            return jsonify(sub.to_dict())

    # ----------------------------------------------------------- problems
    @api.get("/problems")
    def list_problems():
        with session_scope() as session:
            rows = session.scalars(select(Problem).order_by(Problem.id)).all()
            return jsonify(
                [
                    {
                        "id": p.id,
                        "title": p.title,
                        "time_limit_ms": p.time_limit_ms,
                        "memory_limit_kb": p.memory_limit_kb,
                    }
                    for p in rows
                ]
            )

    @api.get("/problems/<int:problem_id>")
    def get_problem(problem_id: int):
        with session_scope() as session:
            p = session.get(Problem, problem_id)
            if p is None:
                return jsonify(error="Problem not found."), 404
            cases = p.test_cases
            return jsonify(
                id=p.id,
                title=p.title,
                description=p.description,
                time_limit_ms=p.time_limit_ms,
                memory_limit_kb=p.memory_limit_kb,
                total_testcases=len(cases),
                sample=cases[0] if cases else None,  # hidden cases are never exposed
            )

    @api.get("/health")
    def health():
        return jsonify(status="ok", **pool.stats())

    app.register_blueprint(api)

    @app.get("/")
    def index():
        return render_template("index.html")

    return app
