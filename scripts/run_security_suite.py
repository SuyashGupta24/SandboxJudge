"""
Phase 5 benchmark: run every adversarial payload through the real sandbox,
print a results table, verify the host was left clean, and report throughput.

    python -m scripts.run_security_suite              # sequential
    python -m scripts.run_security_suite --parallel 4 # also measures throughput

Exit code is non-zero if any verdict is unexpected or anything leaked.
"""
import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from app.config import SANDBOX
from app.executor import CONTAINER_LABEL, SandboxError, SandboxExecutor
from tests.payloads.manifest import CASES, PAYLOADS, load_source


def run_one(executor: SandboxExecutor, payload):
    started = time.monotonic()
    result = executor.judge(
        language=payload.language,
        source_code=load_source(payload),
        testcases=CASES,
        time_limit_ms=payload.time_limit_ms,
    )
    return payload, result, time.monotonic() - started


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parallel", type=int, default=1, help="concurrent judge threads")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)

    executor = SandboxExecutor()
    try:
        executor.ensure_ready()
    except SandboxError as exc:
        print(exc)
        return 2
    executor.reap_orphans()

    wall_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, args.parallel)) as pool:
        outcomes = list(pool.map(lambda p: run_one(executor, p), PAYLOADS))
    wall = time.monotonic() - wall_start

    print(f"\n{'payload':26} {'category':12} {'verdict':18} {'sec':>6}  result")
    print("-" * 78)
    bad = 0
    for payload, result, secs in outcomes:
        ok = result.status in payload.expected
        bad += not ok
        print(f"{payload.name:26} {payload.category:12} {result.status.value:18} {secs:6.2f}  {'OK' if ok else 'UNEXPECTED'}")
        if not ok:
            print(f"    stderr: {result.stderr[:200]!r}")

    leaked_containers = len(executor.client.containers.list(all=True, filters={"label": f"{CONTAINER_LABEL}=1"}))
    scratch = SANDBOX.scratch_root
    leaked_dirs = len(list(scratch.iterdir())) if scratch.exists() else 0
    escapes = sum(1 for p, r, _ in outcomes if p.category == "escape" and r.status.value == "ACCEPTED")

    total = len(outcomes)
    print("-" * 78)
    print(f"payloads passed       : {total - bad}/{total}")
    print(f"sandbox escapes       : {escapes}")
    print(f"leaked containers     : {leaked_containers}")
    print(f"leaked scratch dirs   : {leaked_dirs}")
    print(f"wall time             : {wall:.1f}s with {args.parallel} thread(s) "
          f"({total / wall * 60:.0f} submissions/min)")
    return 0 if (bad == 0 and escapes == 0 and leaked_containers == 0 and leaked_dirs == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
