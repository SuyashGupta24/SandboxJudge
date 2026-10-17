"""
Insert sample problems (idempotent: skips titles that already exist).

    python -m scripts.seed_problems
"""
from sqlalchemy import select

from app.models import Problem, init_db, session_scope

PROBLEMS = [
    {
        "title": "Sum of Two Numbers",
        "description": "Read two integers a and b from one line. Print a + b.",
        "time_limit_ms": 2000,
        "memory_limit_kb": 131072,
        "test_cases": [
            {"input": "2 3\n", "expected_output": "5\n"},
            {"input": "10 -4\n", "expected_output": "6\n"},
            {"input": "0 0\n", "expected_output": "0\n"},
            {"input": "1000000000 1000000000\n", "expected_output": "2000000000\n"},
        ],
    },
    {
        "title": "Reverse a String",
        "description": "Read one line containing a string. Print it reversed.",
        "time_limit_ms": 2000,
        "memory_limit_kb": 131072,
        "test_cases": [
            {"input": "hello\n", "expected_output": "olleh\n"},
            {"input": "a\n", "expected_output": "a\n"},
            {"input": "SandboxJudge\n", "expected_output": "egduJxobdnaS\n"},
        ],
    },
    {
        "title": "Maximum of N Numbers",
        "description": "The first line contains N. The second line contains N integers. Print the maximum.",
        "time_limit_ms": 2000,
        "memory_limit_kb": 131072,
        "test_cases": [
            {"input": "5\n3 9 2 7 1\n", "expected_output": "9\n"},
            {"input": "3\n-5 -2 -9\n", "expected_output": "-2\n"},
            {"input": "1\n42\n", "expected_output": "42\n"},
        ],
    },
]


def main() -> None:
    init_db()
    added = 0
    with session_scope() as session:
        existing = set(session.scalars(select(Problem.title)))
        for spec in PROBLEMS:
            if spec["title"] in existing:
                continue
            problem = Problem(
                title=spec["title"],
                description=spec["description"],
                time_limit_ms=spec["time_limit_ms"],
                memory_limit_kb=spec["memory_limit_kb"],
            )
            problem.test_cases = spec["test_cases"]
            session.add(problem)
            added += 1
    print(f"Seeded {added} new problem(s); {len(PROBLEMS) - added} already present.")


if __name__ == "__main__":
    main()
