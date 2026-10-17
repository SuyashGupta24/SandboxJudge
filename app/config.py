"""Central configuration. Every knob can be overridden via environment variable."""
from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


@dataclass(frozen=True)
class SandboxConfig:
    # --- image / identity -------------------------------------------------
    image: str = os.getenv("RCE_RUNNER_IMAGE", "rce-runner:latest")
    sandbox_user: str = "10001:10001"  # must match Dockerfile.runner
    harness_path: str = "/opt/runner/harness.py"

    # --- scratch space on the HOST (bind-mounted into containers) ---------
    # NOTE: must be a path the Docker daemon can bind-mount. On Docker Desktop
    # (macOS/Windows) point RCE_SCRATCH_DIR at a shared directory.
    scratch_root: Path = field(
        default_factory=lambda: Path(
            os.getenv("RCE_SCRATCH_DIR", str(Path(tempfile.gettempdir()) / "rce-scratch"))
        )
    )

    # --- resource limits (run stage) --------------------------------------
    cpu_cores: float = _env_float("RCE_CPU_CORES", 1.0)  # -> nano_cpus
    run_pids_limit: int = _env_int("RCE_RUN_PIDS", 50)  # fork-bomb guard
    tmpfs_size_mb: int = _env_int("RCE_TMPFS_MB", 64)
    # Memory the supervisor + runtime itself costs inside the cgroup. Added on
    # top of the problem's memory limit so the *problem* limit stays fair.
    container_overhead_mb: int = _env_int("RCE_OVERHEAD_MB", 16)

    # --- resource limits (compile stage) ----------------------------------
    compile_mem_mb: int = _env_int("RCE_COMPILE_MEM_MB", 512)
    compile_pids_limit: int = _env_int("RCE_COMPILE_PIDS", 128)
    compile_timeout_s: float = _env_float("RCE_COMPILE_TIMEOUT_S", 15.0)

    # --- timing -----------------------------------------------------------
    # Host-side kill deadline = time_limit + grace. Grace absorbs container
    # create/start latency, which is NOT charged to the user (the harness
    # measures wall time around the user process only).
    startup_grace_s: float = _env_float("RCE_STARTUP_GRACE_S", 1.5)

    # --- output caps ------------------------------------------------------
    max_output_bytes: int = _env_int("RCE_MAX_OUTPUT_BYTES", 64 * 1024)
    max_file_size_bytes: int = _env_int("RCE_MAX_FILE_BYTES", 16 * 1024 * 1024)

    # --- problem defaults (used for custom-stdin runs) --------------------
    default_time_limit_ms: int = 2000
    default_memory_limit_kb: int = 128 * 1024


@dataclass(frozen=True)
class AppConfig:
    database_url: str = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'rce.db'}")
    worker_count: int = _env_int("RCE_WORKERS", 4)  # used in Phase 3
    queue_maxsize: int = _env_int("RCE_QUEUE_MAX", 1000)  # used in Phase 3
    max_source_bytes: int = _env_int("RCE_MAX_SOURCE_BYTES", 64 * 1024)  # Phase 4


SANDBOX = SandboxConfig()
APP = AppConfig()
