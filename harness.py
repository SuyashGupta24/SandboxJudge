#!/usr/bin/env python3
"""
In-container supervisor ("harness") for the RCE sandbox.

Runs INSIDE the sandbox container as the unprivileged `sandbox` user. It:
  1. launches the target command (compiler or user program) with stdin from a file,
  2. redirects the child's stdout/stderr to size-capped files on the /tmp tmpfs
     (so a program that prints gigabytes cannot flood Docker logs or the host),
  3. applies per-process rlimits (max file size, no core dumps, fd cap),
  4. reaps the child with wait4() to get an exact peak RSS (ru_maxrss) and CPU time,
  5. prints ONE JSON line (the last line on stdout) describing the outcome.

The host trusts only the LAST JSON line: user code can never write after the
harness has emitted its result, because the container is torn down immediately
once the harness exits.

Usage:
    harness.py [--stdin PATH] [--max-output N] [--max-file-size N] -- CMD [ARGS...]
"""
import argparse
import json
import os
import resource
import signal
import subprocess
import sys
import time

STDOUT_PATH = "/tmp/.user_stdout"
STDERR_PATH = "/tmp/.user_stderr"

# Scrubbed environment: the user program never sees anything we did not choose.
CLEAN_ENV = {
    "PATH": "/usr/local/bin:/usr/bin:/bin",
    "HOME": "/tmp",
    "TMPDIR": "/tmp",
    "LANG": "C.UTF-8",
    "PYTHONDONTWRITEBYTECODE": "1",
}


def _make_preexec(max_file_size: int):
    """rlimits applied in the child between fork() and exec()."""

    def _apply() -> None:
        # A write beyond this size raises SIGXFSZ -> we report "output limit exceeded".
        resource.setrlimit(resource.RLIMIT_FSIZE, (max_file_size, max_file_size))
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_NOFILE, (256, 256))

    return _apply


def _read_capped(fd: int, cap: int):
    """Read at most `cap` bytes from the start of fd; report whether it was truncated."""
    size = os.fstat(fd).st_size
    data = os.pread(fd, cap, 0)
    return data.decode("utf-8", errors="replace"), size > cap


def _emit(payload: dict) -> None:
    # Leading newline guarantees the JSON sits on its own line even if the
    # user program wrote a partial line to our stdout.
    sys.stdout.write("\n" + json.dumps(payload) + "\n")
    sys.stdout.flush()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stdin", default="/dev/null")
    parser.add_argument("--max-output", type=int, default=65536)
    parser.add_argument("--max-file-size", type=int, default=16 * 1024 * 1024)
    parser.add_argument("cmd", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd
    if not cmd:
        _emit({"harness_error": "no command supplied"})
        return 0

    out_fd = os.open(STDOUT_PATH, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
    err_fd = os.open(STDERR_PATH, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)

    with open(args.stdin, "rb") as stdin_file:
        start = time.monotonic()
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=stdin_file,
                stdout=out_fd,
                stderr=err_fd,
                cwd="/sandbox",
                env=CLEAN_ENV,
                preexec_fn=_make_preexec(args.max_file_size),
                close_fds=True,
            )
        except OSError as exc:
            _emit({"spawn_error": f"{type(exc).__name__}: {exc}"})
            return 0

        # wait4 returns the child's own rusage (incl. reaped descendants).
        _, status, rusage = os.wait4(proc.pid, 0)
        elapsed = time.monotonic() - start
        proc.returncode = 0  # already reaped; stop Popen from trying again

    if os.WIFSIGNALED(status):
        sig_num = os.WTERMSIG(status)
        exit_code, sig_name = None, signal.Signals(sig_num).name
    else:
        sig_num = None
        exit_code, sig_name = os.WEXITSTATUS(status), None

    stdout_text, stdout_trunc = _read_capped(out_fd, args.max_output)
    stderr_text, stderr_trunc = _read_capped(err_fd, args.max_output)

    _emit(
        {
            "exit_code": exit_code,
            "signal": sig_name,
            "wall_time_ms": int(elapsed * 1000),
            "cpu_time_ms": int((rusage.ru_utime + rusage.ru_stime) * 1000),
            "max_rss_kb": int(rusage.ru_maxrss),  # Linux reports KB
            "stdout": stdout_text,
            "stderr": stderr_text,
            "stdout_truncated": stdout_trunc,
            "stderr_truncated": stderr_trunc,
            "output_limit_exceeded": sig_num == signal.SIGXFSZ,
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
