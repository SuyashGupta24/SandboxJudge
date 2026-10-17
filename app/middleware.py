"""
Phase 4: request validation, in-memory rate limiting, and JSON error handlers.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Optional

from flask import Flask, jsonify, request

from app.config import APP
from app.languages import UnsupportedLanguageError, get_runner


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class ValidationError(Exception):
    """Maps to HTTP 400."""


@dataclass(frozen=True)
class SubmitRequest:
    language: str  # canonical runner name: python | cpp | java
    source_code: str
    problem_id: Optional[int]
    stdin: Optional[str]


def validate_submit_payload(data: Any) -> SubmitRequest:
    if not isinstance(data, dict):
        raise ValidationError("Body must be a JSON object.")

    language = data.get("language")
    if not isinstance(language, str) or not language.strip():
        raise ValidationError("'language' is required (python, cpp or java).")
    try:
        canonical = get_runner(language).name
    except UnsupportedLanguageError as exc:
        raise ValidationError(str(exc)) from None

    source = data.get("source_code")
    if not isinstance(source, str) or not source.strip():
        raise ValidationError("'source_code' must be a non-empty string.")
    if "\x00" in source:
        raise ValidationError("'source_code' must not contain NUL bytes.")
    if len(source.encode("utf-8")) > APP.max_source_bytes:
        raise ValidationError(f"'source_code' exceeds {APP.max_source_bytes} bytes.")

    problem_id = data.get("problem_id")
    has_stdin = "stdin" in data
    if problem_id is not None and has_stdin:
        raise ValidationError("Provide either 'problem_id' or 'stdin', not both.")
    if problem_id is None and not has_stdin:
        raise ValidationError("Provide 'problem_id' or a custom 'stdin'.")

    stdin: Optional[str] = None
    if problem_id is not None:
        if isinstance(problem_id, bool) or not isinstance(problem_id, int) or problem_id < 1:
            raise ValidationError("'problem_id' must be a positive integer.")
    else:
        stdin = data["stdin"]
        if not isinstance(stdin, str):
            raise ValidationError("'stdin' must be a string.")
        if "\x00" in stdin or len(stdin.encode("utf-8")) > APP.max_stdin_bytes:
            raise ValidationError(f"'stdin' must be text and at most {APP.max_stdin_bytes} bytes.")

    return SubmitRequest(canonical, source, problem_id, stdin)


# ---------------------------------------------------------------------------
# Rate limiting (sliding window, per client key, thread-safe)
# ---------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, max_requests: int, window_s: float):
        self.max_requests = max_requests
        self.window_s = window_s
        self._hits: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def check(self, key: str) -> tuple[bool, int]:
        """Return (allowed, retry_after_seconds)."""
        now = time.monotonic()
        with self._lock:
            hits = self._hits.setdefault(key, deque())
            while hits and now - hits[0] >= self.window_s:
                hits.popleft()
            if len(hits) >= self.max_requests:
                return False, max(1, int(self.window_s - (now - hits[0])) + 1)
            hits.append(now)
            if len(self._hits) > 10_000:  # bound memory: drop idle keys
                self._prune(now)
            return True, 0

    def _prune(self, now: float) -> None:
        for k in [k for k, d in self._hits.items() if not d or now - d[-1] >= self.window_s]:
            del self._hits[k]


def rate_limited(limiter: RateLimiter) -> Callable:
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            allowed, retry_after = limiter.check(request.remote_addr or "unknown")
            if not allowed:
                resp = jsonify(error="Rate limit exceeded.", retry_after_seconds=retry_after)
                resp.status_code = 429
                resp.headers["Retry-After"] = str(retry_after)
                return resp
            return fn(*args, **kwargs)

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# JSON error handlers
# ---------------------------------------------------------------------------
def register_error_handlers(app: Flask) -> None:
    @app.errorhandler(ValidationError)
    def _bad_request(exc):
        return jsonify(error=str(exc)), 400

    @app.errorhandler(404)
    def _not_found(_exc):
        return jsonify(error="Not found."), 404

    @app.errorhandler(405)
    def _method_not_allowed(_exc):
        return jsonify(error="Method not allowed."), 405

    @app.errorhandler(413)
    def _too_large(_exc):
        return jsonify(error="Request body too large."), 413

    @app.errorhandler(500)
    def _server_error(_exc):
        return jsonify(error="Internal server error."), 500
