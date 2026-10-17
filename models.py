"""
Database layer: schema, submission state machine, and session helpers.

Works unchanged on SQLite (default) and PostgreSQL (set DATABASE_URL).
"""
from __future__ import annotations

import enum
import json
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Optional

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    create_engine,
    event,
    select,
    update,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)

from app.config import APP


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------
class SubmissionStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    ACCEPTED = "ACCEPTED"
    WRONG_ANSWER = "WRONG_ANSWER"
    TLE = "TLE"
    MLE = "MLE"
    COMPILATION_ERROR = "COMPILATION_ERROR"
    RUNTIME_ERROR = "RUNTIME_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"  # sandbox/infra failure, never the user's fault


TERMINAL_STATUSES = frozenset(
    {
        SubmissionStatus.ACCEPTED,
        SubmissionStatus.WRONG_ANSWER,
        SubmissionStatus.TLE,
        SubmissionStatus.MLE,
        SubmissionStatus.COMPILATION_ERROR,
        SubmissionStatus.RUNTIME_ERROR,
        SubmissionStatus.INTERNAL_ERROR,
    }
)

# PENDING -> RUNNING -> <terminal>;  RUNNING -> PENDING only for crash/retry requeue.
_ALLOWED_TRANSITIONS = {
    SubmissionStatus.PENDING: {SubmissionStatus.RUNNING},
    SubmissionStatus.RUNNING: TERMINAL_STATUSES | {SubmissionStatus.PENDING},
}


class InvalidTransition(Exception):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
class Base(DeclarativeBase):
    pass


class Problem(Base):
    __tablename__ = "problems"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    time_limit_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=2000)
    memory_limit_kb: Mapped[int] = mapped_column(Integer, nullable=False, default=128 * 1024)
    # JSON list of {"input": str, "expected_output": str}
    test_cases_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")

    submissions: Mapped[list["Submission"]] = relationship(back_populates="problem")

    @property
    def test_cases(self) -> list[dict[str, str]]:
        return json.loads(self.test_cases_json)

    @test_cases.setter
    def test_cases(self, cases: list[dict[str, str]]) -> None:
        self.test_cases_json = json.dumps(cases)


class Submission(Base):
    __tablename__ = "submissions"
    __table_args__ = (Index("ix_submissions_status_created", "status", "created_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    # Nullable: a "custom stdin" run has no problem attached.
    problem_id: Mapped[Optional[int]] = mapped_column(ForeignKey("problems.id"), nullable=True)
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    source_code: Mapped[str] = mapped_column(Text, nullable=False)
    custom_stdin: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default=SubmissionStatus.PENDING.value, index=True
    )
    stdout: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stderr: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    execution_time_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    memory_used_kb: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    passed_testcases: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_testcases: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    problem: Mapped[Optional[Problem]] = relationship(back_populates="submissions")

    # -- state machine ------------------------------------------------------
    @property
    def status_enum(self) -> SubmissionStatus:
        return SubmissionStatus(self.status)

    def transition_to(self, new_status: SubmissionStatus) -> None:
        """Validated in-memory transition. Caller commits."""
        current = self.status_enum
        if new_status not in _ALLOWED_TRANSITIONS.get(current, set()):
            raise InvalidTransition(f"{current.value} -> {new_status.value} is not allowed")
        self.status = new_status.value
        if new_status in TERMINAL_STATUSES:
            self.finished_at = _utcnow()

    def to_dict(self) -> dict[str, Any]:
        return {
            "submission_id": self.id,
            "problem_id": self.problem_id,
            "language": self.language,
            "status": self.status,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "execution_time_ms": self.execution_time_ms,
            "memory_used_kb": self.memory_used_kb,
            "passed_testcases": self.passed_testcases,
            "total_testcases": self.total_testcases,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# ---------------------------------------------------------------------------
# Atomic queue-claim helpers (safe with many worker threads / processes)
# ---------------------------------------------------------------------------
def claim_submission(session: Session, submission_id: str) -> bool:
    """
    Atomically flip PENDING -> RUNNING. Returns True only for the single caller
    that wins the race, so a job can never be executed twice.
    """
    result = session.execute(
        update(Submission)
        .where(Submission.id == submission_id, Submission.status == SubmissionStatus.PENDING.value)
        .values(status=SubmissionStatus.RUNNING.value, started_at=_utcnow())
    )
    session.commit()
    return result.rowcount == 1


def recover_stale_running(session: Session) -> list[str]:
    """Startup recovery: anything left RUNNING by a crashed process goes back to PENDING."""
    stale = list(
        session.scalars(select(Submission.id).where(Submission.status == SubmissionStatus.RUNNING.value))
    )
    if stale:
        session.execute(
            update(Submission)
            .where(Submission.id.in_(stale))
            .values(status=SubmissionStatus.PENDING.value, started_at=None)
        )
        session.commit()
    return stale


# ---------------------------------------------------------------------------
# Engine / session management
# ---------------------------------------------------------------------------
_engine: Optional[Engine] = None
_session_factory: Optional[sessionmaker] = None
_init_lock = threading.Lock()


def init_db(database_url: Optional[str] = None) -> Engine:
    """Create the engine (once), enable SQLite WAL, and create tables."""
    global _engine, _session_factory
    with _init_lock:
        if _engine is not None:
            return _engine

        url = database_url or APP.database_url
        is_sqlite = url.startswith("sqlite")
        kwargs: dict[str, Any] = {"pool_pre_ping": True}
        if is_sqlite:
            # Worker threads share the engine; SQLite connections must cross threads.
            kwargs["connect_args"] = {"check_same_thread": False, "timeout": 30}

        engine = create_engine(url, **kwargs)

        if is_sqlite:

            @event.listens_for(engine, "connect")
            def _sqlite_pragmas(dbapi_conn, _record):  # pragma: no cover
                cur = dbapi_conn.cursor()
                cur.execute("PRAGMA journal_mode=WAL")  # readers don't block the writer
                cur.execute("PRAGMA synchronous=NORMAL")
                cur.execute("PRAGMA foreign_keys=ON")
                cur.close()

        Base.metadata.create_all(engine)
        _engine = engine
        _session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        return engine


@contextmanager
def session_scope() -> Iterator[Session]:
    """One short-lived session per unit of work; commit on success, rollback on error."""
    if _session_factory is None:
        init_db()
    assert _session_factory is not None
    session = _session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
