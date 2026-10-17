"""
Quick end-to-end check of Phase 2 (full payload suite comes in Phase 5).

    python -m scripts.smoke_test_executor
"""
import logging
import sys

from app.executor import SandboxExecutor
from app.models import SubmissionStatus as S

CASES = [
    {"input": "2 3\n", "expected_output": "5\n"},
    {"input": "10 -4\n", "expected_output": "6\n"},
]

PY_OK = "a, b = map(int, input().split())\nprint(a + b)\n"
CPP_OK = """#include <iostream>
int main(){ long long a,b; std::cin>>a>>b; std::cout<<a+b<<"\\n"; }
"""
JAVA_OK = """import java.util.*;
public class Solution {
    public static void main(String[] args) {
        Scanner s = new Scanner(System.in);
        System.out.println(s.nextInt() + s.nextInt());
    }
}
"""

# (label, language, source, acceptable statuses)
PAYLOADS = [
    ("python AC", "python", PY_OK, {S.ACCEPTED}),
    ("cpp AC", "cpp", CPP_OK, {S.ACCEPTED}),
    ("java AC", "java", JAVA_OK, {S.ACCEPTED}),
    ("python WA", "python", "print(0)\n", {S.WRONG_ANSWER}),
    ("python runtime error", "python", "raise SystemExit(3)\n", {S.RUNTIME_ERROR}),
    ("cpp compile error", "cpp", "int main( { return 0; }\n", {S.COMPILATION_ERROR}),
    ("python infinite loop", "python", "while True:\n    pass\n", {S.TLE}),
    ("python memory hog", "python", "x = []\nwhile True:\n    x.append(bytearray(10**7))\n", {S.MLE}),
    ("python fork bomb", "python", "import os\nwhile True:\n    os.fork()\n", {S.TLE, S.RUNTIME_ERROR}),
    ("python network attempt", "python",
     "import socket\ns = socket.socket()\ns.connect(('1.1.1.1', 80))\nprint('ESCAPED')\n", {S.RUNTIME_ERROR}),
    ("python write to rootfs", "python",
     "open('/etc/pwned', 'w').write('x')\nprint('ESCAPED')\n", {S.RUNTIME_ERROR}),
]


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    executor = SandboxExecutor()
    executor.ensure_ready()
    executor.reap_orphans()

    failures = 0
    print(f"{'payload':28} {'status':18} {'ms':>6} {'KB':>8}  result")
    for label, lang, source, expected in PAYLOADS:
        res = executor.judge(language=lang, source_code=source, testcases=CASES, time_limit_ms=1000)
        ok = res.status in expected
        failures += not ok
        print(f"{label:28} {res.status.value:18} {res.execution_time_ms:>6} {res.memory_used_kb:>8}  {'OK' if ok else 'UNEXPECTED'}")
        if not ok:
            print(f"    stderr: {res.stderr[:300]!r}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
