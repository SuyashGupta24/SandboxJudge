"""
Sandbox execution engine.

Every compile step and every test-case run happens in its OWN ephemeral Docker
container, which is force-removed in a `finally` block no matter what happens.

Isolation applied to every container:
  * no network                      (network_disabled + network_mode="none")
  * memory + swap hard cap          (mem_limit == memswap_limit -> no swap, OOM-kill)
  * CPU cap                         (nano_cpus)
  * fork-bomb guard                 (pids_limit)
  * read-only root filesystem       (read_only=True) + small noexec tmpfs on /tmp
  * all Linux capabilities dropped  (cap_drop=["ALL"]) + no-new-privileges
  * unprivileged uid:gid            (10001:10001)
  * source/input bind-mounted       (read-only for the run stage)
  * bounded container logs          (json-file max-size) and bounded output files
  * host-side wall-clock deadline   (container.wait(timeout) -> kill)
"""
from __future__ import annotations

import json
import logging
import math
import os
import shutil
import tempfile
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

import docker
from docker.errors import APIError, DockerException, ImageNotFound
from docker.types import LogConfig
from requests.exceptions import RequestException

from app.config import SANDBOX, SandboxConfig
from app.languages import LanguageRunner, get_runner
from app.models import SubmissionStatus

logger = logging.getLogger(__name__)

SANDBOX_DIR = "/sandbox"
INPUT_FILENAME = "input.txt"
CONTAINER_LABEL = "rce.sandbox"


class SandboxError(RuntimeError):
    """Infrastructure failure (Docker down, harness crashed). NOT the user's fault."""


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------
@dataclass
class ContainerResult:
    """Raw outcome of one container run."""

    exit_code: Optional[int] = None
    signal: Optional[str] = None
    wall_time_ms: int = 0
    cpu_time_ms: int = 0
    max_rss_kb: int = 0
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False
    oom_killed: bool = False
    output_limit_exceeded: bool = False


@dataclass
class JudgeResult:
    """Final verdict for a submission; maps 1:1 onto the `submissions` row."""

    status: SubmissionStatus
    stdout: str = ""
    stderr: str = ""
    execution_time_ms: int = 0  # worst case across test cases
    memory_used_kb: int = 0  # worst case across test cases
    passed_testcases: int = 0
    total_testcases: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {**self.__dict__, "status": self.status.value}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def outputs_match(actual: str, expected: str) -> bool:
    """Judge-style comparison: ignore trailing whitespace per line and trailing blank lines."""

    def norm(text: str) -> list[str]:
        return [line.rstrip() for line in text.replace("\r\n", "\n").rstrip().split("\n")]

    return norm(actual) == norm(expected)


def _write_file(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    os.chmod(path, 0o644)  # sandbox uid (10001) needs read access


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------
class SandboxExecutor:
    """Thread-safe: one instance can be shared by every worker thread."""

    def __init__(self, config: SandboxConfig = SANDBOX, client: Optional[docker.DockerClient] = None):
        self.cfg = config
        self._client = client
        self._client_lock = threading.Lock()

    # -- docker plumbing ------------------------------------------------------
    @property
    def client(self) -> docker.DockerClient:
        with self._client_lock:
            if self._client is None:
                self._client = docker.from_env()
            return self._client

    def ensure_ready(self) -> None:
        """Fail fast at startup if Docker or the runner image is missing."""
        try:
            self.client.ping()
            self.client.images.get(self.cfg.image)
        except ImageNotFound:
            raise SandboxError(
                f"Runner image {self.cfg.image!r} not found. Build it with:\n"
                f"  docker build -f docker/Dockerfile.runner -t {self.cfg.image} ."
            ) from None
        except DockerException as exc:
            raise SandboxError(f"Docker is not reachable: {exc}") from exc

    def reap_orphans(self) -> int:
        """Remove sandbox containers leaked by a crashed host process (call at startup)."""
        removed = 0
        for container in self.client.containers.list(all=True, filters={"label": f"{CONTAINER_LABEL}=1"}):
            try:
                container.remove(force=True, v=True)
                removed += 1
            except APIError:
                logger.warning("could not remove orphan container %s", container.short_id)
        return removed

    # -- host scratch workspace ---------------------------------------------
    @contextmanager
    def _workspace(self) -> Iterator[Path]:
        """Per-submission temp dir, bind-mounted into the containers, always cleaned up."""
        self.cfg.scratch_root.mkdir(parents=True, exist_ok=True)
        path = Path(tempfile.mkdtemp(prefix="sub-", dir=self.cfg.scratch_root))
        os.chmod(path, 0o777)  # compile stage runs as uid 10001 and must write here
        try:
            yield path
        finally:
            shutil.rmtree(path, ignore_errors=True)
            if path.exists():
                # Files owned by uid 10001 inside a sub-directory can resist rmtree
                # when the host user differs; a periodic janitor should sweep these.
                logger.warning("scratch dir %s could not be fully removed", path)

    # -- one hardened container run ------------------------------------------
    def _run_in_container(
        self,
        *,
        command: list[str],
        workdir: Path,
        writable: bool,
        stdin_path: str,
        mem_limit_mb: int,
        pids_limit: int,
        timeout_s: float,
    ) -> ContainerResult:
        harness_cmd = [
            "python3",
            self.cfg.harness_path,
            "--stdin", stdin_path,
            "--max-output", str(self.cfg.max_output_bytes),
            "--max-file-size", str(self.cfg.max_file_size_bytes),
            "--",
            *command,
        ]

        container = None
        timed_out = False
        try:
            container = self.client.containers.run(
                self.cfg.image,
                harness_cmd,
                detach=True,
                name=f"rce-{uuid.uuid4().hex[:12]}",
                labels={CONTAINER_LABEL: "1"},
                # --- isolation -------------------------------------------------
                network_disabled=True,
                network_mode="none",
                read_only=True,
                tmpfs={"/tmp": f"rw,noexec,nosuid,nodev,size={self.cfg.tmpfs_size_mb}m,mode=1777"},
                cap_drop=["ALL"],
                security_opt=["no-new-privileges"],
                user=self.cfg.sandbox_user,
                working_dir=SANDBOX_DIR,
                init=True,  # tini as PID 1: reaps orphaned fork-bomb children
                # --- cgroup limits ---------------------------------------------
                mem_limit=f"{mem_limit_mb}m",
                memswap_limit=f"{mem_limit_mb}m",  # == mem_limit -> swap disabled
                nano_cpus=int(self.cfg.cpu_cores * 1_000_000_000),
                pids_limit=pids_limit,
                # --- I/O -------------------------------------------------------
                volumes={str(workdir): {"bind": SANDBOX_DIR, "mode": "rw" if writable else "ro"}},
                log_config=LogConfig(type=LogConfig.types.JSON, config={"max-size": "1m", "max-file": "1"}),
            )

            try:
                container.wait(timeout=timeout_s)
            except RequestException:
                # docker-py surfaces a client-side read timeout as a requests error.
                timed_out = True
                try:
                    container.kill()
                    container.wait(timeout=5)
                except (APIError, RequestException):
                    pass  # already exited / being torn down; removal below is forced anyway

            container.reload()
            state = container.attrs.get("State", {})
            oom_killed = bool(state.get("OOMKilled"))
            logs = container.logs(stdout=True, stderr=True, tail=20).decode("utf-8", errors="replace")

            if timed_out:
                return ContainerResult(timed_out=True, oom_killed=oom_killed)

            data = self._parse_harness_output(logs)
            if data is None:
                if oom_killed:  # the OOM killer took out the supervisor itself
                    return ContainerResult(oom_killed=True)
                raise SandboxError(
                    f"harness produced no result (container exit code {state.get('ExitCode')}): {logs[-500:]!r}"
                )
            if "spawn_error" in data or "harness_error" in data:
                # e.g. the compiler/binary doesn't exist -> surface as a failed process
                return ContainerResult(
                    exit_code=127, stderr=data.get("spawn_error") or data.get("harness_error", ""), oom_killed=oom_killed
                )

            return ContainerResult(
                exit_code=data.get("exit_code"),
                signal=data.get("signal"),
                wall_time_ms=int(data.get("wall_time_ms", 0)),
                cpu_time_ms=int(data.get("cpu_time_ms", 0)),
                max_rss_kb=int(data.get("max_rss_kb", 0)),
                stdout=data.get("stdout", ""),
                stderr=data.get("stderr", ""),
                oom_killed=oom_killed,
                output_limit_exceeded=bool(data.get("output_limit_exceeded")),
            )
        except DockerException as exc:
            raise SandboxError(f"Docker failure: {exc}") from exc
        finally:
            if container is not None:
                try:
                    container.remove(force=True, v=True)  # ephemeral: always destroyed
                except (APIError, RequestException):
                    logger.warning("failed to remove container %s", container.short_id)

    @staticmethod
    def _parse_harness_output(raw_logs: str) -> Optional[dict[str, Any]]:
        """Trust only the LAST well-formed JSON line; user code can't write after it."""
        for line in reversed(raw_logs.strip().splitlines()):
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue
        return None

    # -- verdict classification ----------------------------------------------
    @staticmethod
    def _classify(
        runner: LanguageRunner, raw: ContainerResult, time_limit_ms: int, memory_limit_kb: int
    ) -> tuple[Optional[SubmissionStatus], str]:
        """Return (failure_status, note) or (None, "") if the process ran cleanly."""
        if raw.oom_killed:
            return SubmissionStatus.MLE, "Memory limit exceeded (killed by the kernel OOM killer)."
        if raw.timed_out or raw.wall_time_ms > time_limit_ms:
            return SubmissionStatus.TLE, f"Time limit exceeded (limit {time_limit_ms} ms)."
        if raw.output_limit_exceeded:
            return SubmissionStatus.RUNTIME_ERROR, "Output limit exceeded."
        if runner.is_memory_error(raw.stderr):
            return SubmissionStatus.MLE, "Memory limit exceeded (runtime reported out-of-memory)."
        if runner.check_rss_limit and raw.max_rss_kb > memory_limit_kb:
            return SubmissionStatus.MLE, f"Memory limit exceeded ({raw.max_rss_kb} KB > {memory_limit_kb} KB)."
        if raw.exit_code != 0:
            return (
                SubmissionStatus.RUNTIME_ERROR,
                f"Process terminated abnormally (exit code {raw.exit_code}, signal {raw.signal}).",
            )
        return None, ""

    # -- public API -----------------------------------------------------------
    def judge(
        self,
        *,
        language: str,
        source_code: str,
        testcases: list[dict[str, str]],
        time_limit_ms: Optional[int] = None,
        memory_limit_kb: Optional[int] = None,
    ) -> JudgeResult:
        """
        Compile (if needed) and run `source_code` against every test case,
        stopping at the first failure.

        testcases: [{"input": str, "expected_output": str}, ...]. A case without
        "expected_output" is a plain run (custom stdin): it passes if it exits cleanly.

        Raises UnsupportedLanguageError for unknown languages (validate in the API).
        Infrastructure failures are returned as INTERNAL_ERROR, never raised.
        """
        runner = get_runner(language)
        time_limit_ms = time_limit_ms or self.cfg.default_time_limit_ms
        memory_limit_kb = memory_limit_kb or self.cfg.default_memory_limit_kb
        try:
            return self._judge(runner, source_code, testcases, time_limit_ms, memory_limit_kb)
        except (SandboxError, DockerException) as exc:
            logger.exception("sandbox failure while judging")
            return JudgeResult(
                status=SubmissionStatus.INTERNAL_ERROR,
                stderr=f"Sandbox failure: {exc}",
                total_testcases=len(testcases),
            )

    def _judge(
        self,
        runner: LanguageRunner,
        source_code: str,
        testcases: list[dict[str, str]],
        time_limit_ms: int,
        memory_limit_kb: int,
    ) -> JudgeResult:
        total = len(testcases)
        with self._workspace() as ws:
            _write_file(ws / runner.source_filename, source_code)

            # ---- stage 1: compile (C++ / Java) -----------------------------
            compile_cmd = runner.compile_command()
            if compile_cmd:
                comp = self._run_in_container(
                    command=compile_cmd,
                    workdir=ws,
                    writable=True,  # the ONLY stage with a writable mount
                    stdin_path="/dev/null",
                    mem_limit_mb=self.cfg.compile_mem_mb,
                    pids_limit=self.cfg.compile_pids_limit,
                    timeout_s=self.cfg.compile_timeout_s + self.cfg.startup_grace_s,
                )
                if comp.timed_out:
                    msg = f"Compilation timed out after {self.cfg.compile_timeout_s:.0f}s."
                elif comp.oom_killed:
                    msg = "Compiler exceeded its memory limit."
                elif comp.exit_code != 0:
                    msg = comp.stderr or comp.stdout or "Compilation failed."
                else:
                    msg = ""
                if msg:
                    return JudgeResult(SubmissionStatus.COMPILATION_ERROR, stderr=msg, total_testcases=total)

            # ---- stage 2: run each test case in a fresh container ------------
            run_cmd = runner.run_command(memory_limit_kb)
            mem_limit_mb = math.ceil(memory_limit_kb / 1024) + self.cfg.container_overhead_mb + runner.extra_memory_mb
            timeout_s = time_limit_ms / 1000 + self.cfg.startup_grace_s

            passed = 0
            worst_time = 0
            worst_mem = 0
            last_stdout = ""
            last_stderr = ""

            for idx, case in enumerate(testcases, start=1):
                # Only the CURRENT input exists in the sandbox: user code can't peek at other cases.
                _write_file(ws / INPUT_FILENAME, case.get("input", ""))

                raw = self._run_in_container(
                    command=run_cmd,
                    workdir=ws,
                    writable=False,  # source + binary are read-only at run time
                    stdin_path=f"{SANDBOX_DIR}/{INPUT_FILENAME}",
                    mem_limit_mb=mem_limit_mb,
                    pids_limit=self.cfg.run_pids_limit,
                    timeout_s=timeout_s,
                )

                if raw.timed_out:
                    worst_time = max(worst_time, time_limit_ms)
                elif raw.oom_killed:
                    worst_mem = max(worst_mem, memory_limit_kb)
                    worst_time = max(worst_time, raw.wall_time_ms)
                else:
                    worst_time = max(worst_time, raw.wall_time_ms)
                    worst_mem = max(worst_mem, raw.max_rss_kb)
                last_stdout, last_stderr = raw.stdout, raw.stderr

                failure, note = self._classify(runner, raw, time_limit_ms, memory_limit_kb)
                if failure is None and "expected_output" in case and not outputs_match(raw.stdout, case["expected_output"]):
                    failure, note = SubmissionStatus.WRONG_ANSWER, f"Wrong answer on test case {idx}."
                if failure is not None:
                    return JudgeResult(
                        status=failure,
                        stdout=raw.stdout,
                        stderr=(raw.stderr + "\n" + f"[test case {idx}/{total}] {note}").strip(),
                        execution_time_ms=worst_time,
                        memory_used_kb=worst_mem,
                        passed_testcases=passed,
                        total_testcases=total,
                    )
                passed += 1

            return JudgeResult(
                status=SubmissionStatus.ACCEPTED,
                stdout=last_stdout,
                stderr=last_stderr,
                execution_time_ms=worst_time,
                memory_used_kb=worst_mem,
                passed_testcases=passed,
                total_testcases=total,
            )
