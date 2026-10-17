"""
Phase 3: thread-safe producer-consumer worker pool (no external broker).

  producers  : Flask request threads call `pool.enqueue(submission_id)`
  queue      : bounded `queue.Queue` (back-pressure instead of unbounded memory)
  consumers  : N daemon threads, each running one submission at a time

Guarantees
  * exactly-once execution   - the DB claim (PENDING -> RUNNING) is atomic, so a
                               duplicate queue entry is simply skipped.
  * no DB lock held during   - the DB session is closed BEFORE the (slow) Docker
    sandbox work              run, which matters for SQLite's single writer.
  * error recovery           - infra failures (INTERNAL_ERROR) are retried with
                               backoff; unexpected exceptions mark the submission
                               INTERNAL_ERROR instead of leaving it RUNNING forever.
  * graceful shutdown        - in-flight jobs finish; queued jobs stay PENDING in
                               the DB and are re-queued by `recover()` next start.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import select

from app.config import APP
from app.executor import JudgeResult, SandboxExecutor
from app.models import (
    InvalidTransition,
    Problem,
    Submission,
    SubmissionStatus,
    claim_submission,
    recover_stale_running,
    session_scope,
)

logger = logging.getLogger(__name__)


class PoolClosed(RuntimeError):
    """Raised when enqueueing after shutdown() has begun."""


@dataclass(frozen=True)
class Job:
    submission_id: str
    attempt: int = 0


class WorkerPool:
    def __init__(
        self,
        executor: SandboxExecutor,
        num_workers: int = APP.worker_count,
        queue_maxsize: int = APP.queue_maxsize,
        max_retries: int = APP.max_retries,
    ):
        self._executor = executor
        self._num_workers = num_workers
        self._max_retries = max_retries
        self._queue: "queue.Queue[Job]" = queue.Queue(maxsize=queue_maxsize)
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._stats_lock = threading.Lock()
        self._active = 0
        self._processed = 0

    # ------------------------------------------------------------------ API
    def start(self) -> None:
        if self._threads:
            return
        for i in range(self._num_workers):
            thread = threading.Thread(target=self._worker_loop, name=f"judge-worker-{i}", daemon=True)
            thread.start()
            self._threads.append(thread)
        logger.info("worker pool started with %d workers", self._num_workers)

    def enqueue(self, submission_id: str, attempt: int = 0) -> None:
        """Non-blocking. Raises queue.Full (caller returns 503) or PoolClosed."""
        if self._stop.is_set():
            raise PoolClosed("worker pool is shutting down")
        self._queue.put_nowait(Job(submission_id, attempt))

    def recover(self) -> int:
        """
        Call once at startup, BEFORE start(): put crashed RUNNING jobs back to
        PENDING and re-queue every PENDING submission, oldest first.
        """
        with session_scope() as session:
            revived = recover_stale_running(session)
            pending = list(
                session.scalars(
                    select(Submission.id)
                    .where(Submission.status == SubmissionStatus.PENDING.value)
                    .order_by(Submission.created_at)
                )
            )
        queued = 0
        for submission_id in pending:
            try:
                self.enqueue(submission_id)
                queued += 1
            except queue.Full:
                logger.warning("queue full during recovery; remaining jobs stay PENDING")
                break
        if revived or queued:
            logger.info("recovery: %d stale RUNNING reset, %d jobs re-queued", len(revived), queued)
        return queued

    def shutdown(self, timeout: float = 30.0) -> bool:
        """Stop accepting work, let in-flight jobs finish. Idempotent. True if all workers exited."""
        self._stop.set()
        deadline = time.monotonic() + timeout
        for thread in self._threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        alive = [t.name for t in self._threads if t.is_alive()]
        left = self._queue.qsize()
        if alive:
            logger.error("shutdown timed out; still running: %s", alive)
        logger.info("worker pool stopped (%d queued jobs remain PENDING in the DB)", left)
        return not alive

    def stats(self) -> dict[str, Any]:
        with self._stats_lock:
            active, processed = self._active, self._processed
        return {
            "workers": self._num_workers,
            "workers_alive": sum(t.is_alive() for t in self._threads),
            "active_jobs": active,
            "queue_depth": self._queue.qsize(),
            "processed_total": processed,
            "accepting": not self._stop.is_set(),
        }

    # ------------------------------------------------------------- internals
    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                job = self._queue.get(timeout=0.5)  # timeout lets us notice _stop
            except queue.Empty:
                continue
            with self._stats_lock:
                self._active += 1
            try:
                self._handle(job)
            except Exception as exc:  # a worker thread must never die
                logger.exception("unhandled error for submission %s", job.submission_id)
                self._mark_internal_error(job.submission_id, f"Worker error: {type(exc).__name__}: {exc}")
            finally:
                with self._stats_lock:
                    self._active -= 1
                    self._processed += 1
                self._queue.task_done()

    def _handle(self, job: Job) -> None:
        # 1) Claim + read inputs, then CLOSE the session before the slow sandbox run.
        with session_scope() as session:
            if not claim_submission(session, job.submission_id):
                logger.info("submission %s already claimed or gone; skipping", job.submission_id)
                return
            sub = session.get(Submission, job.submission_id)
            language, source = sub.language, sub.source_code
            if sub.problem_id is not None:
                problem = session.get(Problem, sub.problem_id)
                testcases = problem.test_cases
                time_limit_ms, memory_limit_kb = problem.time_limit_ms, problem.memory_limit_kb
            else:  # custom-stdin run: no expected output, so it "passes" if it exits cleanly
                testcases = [{"input": sub.custom_stdin or ""}]
                time_limit_ms = memory_limit_kb = None

        # 2) Sandbox work (no DB connection held).
        result = self._executor.judge(
            language=language,
            source_code=source,
            testcases=testcases,
            time_limit_ms=time_limit_ms,
            memory_limit_kb=memory_limit_kb,
        )

        # 3) Retry infra failures, otherwise persist the verdict.
        if result.status is SubmissionStatus.INTERNAL_ERROR and job.attempt < self._max_retries:
            self._retry(job, result)
        else:
            self._persist(job.submission_id, result)

    def _persist(self, submission_id: str, result: JudgeResult) -> None:
        with session_scope() as session:
            sub = session.get(Submission, submission_id)
            if sub is None:
                return
            sub.stdout = result.stdout
            sub.stderr = result.stderr
            sub.execution_time_ms = result.execution_time_ms
            sub.memory_used_kb = result.memory_used_kb
            sub.passed_testcases = result.passed_testcases
            sub.total_testcases = result.total_testcases
            sub.transition_to(result.status)

    def _retry(self, job: Job, result: JudgeResult) -> None:
        logger.warning(
            "infra failure on %s (attempt %d/%d): %s",
            job.submission_id, job.attempt + 1, self._max_retries, result.stderr[:200],
        )
        with session_scope() as session:
            sub = session.get(Submission, job.submission_id)
            sub.transition_to(SubmissionStatus.PENDING)  # RUNNING -> PENDING (allowed requeue edge)
            sub.started_at = None
        if self._stop.wait(min(2 ** job.attempt, 5)):  # interruptible backoff
            return  # shutting down: stays PENDING, recovered on next start
        try:
            self.enqueue(job.submission_id, job.attempt + 1)
        except PoolClosed:
            pass
        except queue.Full:
            self._fail_pending(job.submission_id, "Queue full while retrying after an infrastructure failure.")

    def _fail_pending(self, submission_id: str, message: str) -> None:
        """Terminate a submission that is currently PENDING (claim it, then fail it)."""
        with session_scope() as session:
            claimed = claim_submission(session, submission_id)
        if claimed:
            self._mark_internal_error(submission_id, message)

    def _mark_internal_error(self, submission_id: str, message: str) -> None:
        try:
            with session_scope() as session:
                sub = session.get(Submission, submission_id)
                if sub is None or sub.status != SubmissionStatus.RUNNING.value:
                    return
                sub.stderr = message
                sub.transition_to(SubmissionStatus.INTERNAL_ERROR)
        except (InvalidTransition, Exception):  # last-resort handler must not raise
            logger.exception("could not mark %s as INTERNAL_ERROR", submission_id)
