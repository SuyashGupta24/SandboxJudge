"""
Language runners (Strategy Pattern).

Each runner only describes *what* to execute; the executor decides *how* to
sandbox it. Adding a language = subclass LanguageRunner + register it below.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional


class UnsupportedLanguageError(ValueError):
    pass


class LanguageRunner(ABC):
    name: str
    source_filename: str

    # Extra cgroup memory on top of the problem limit (e.g. JVM metaspace/threads).
    extra_memory_mb: int = 0
    # Whether peak RSS vs. the problem limit is a meaningful MLE signal.
    # (False for the JVM: its baseline RSS is unrelated to the user's heap.)
    check_rss_limit: bool = True
    # Substrings in stderr that mean "the runtime itself ran out of memory".
    memory_error_markers: tuple[str, ...] = ()

    @abstractmethod
    def compile_command(self) -> Optional[list[str]]:
        """Command run in the compile stage, or None for interpreted languages."""

    @abstractmethod
    def run_command(self, memory_limit_kb: int) -> list[str]:
        """Command run once per test case (stdin is wired up by the harness)."""

    def is_memory_error(self, stderr: str) -> bool:
        return any(marker in stderr for marker in self.memory_error_markers)


class PythonRunner(LanguageRunner):
    name = "python"
    source_filename = "solution.py"
    memory_error_markers = ("MemoryError",)

    def compile_command(self) -> Optional[list[str]]:
        return None

    def run_command(self, memory_limit_kb: int) -> list[str]:
        # -I: isolated mode (ignore PYTHON* env vars and user site-packages).
        return ["python3", "-I", self.source_filename]


class CppRunner(LanguageRunner):
    name = "cpp"
    source_filename = "solution.cpp"
    memory_error_markers = ("std::bad_alloc",)

    def compile_command(self) -> Optional[list[str]]:
        return ["g++", "-O3", "-std=c++17", "-pipe", "-o", "solution", self.source_filename]

    def run_command(self, memory_limit_kb: int) -> list[str]:
        return ["./solution"]


class JavaRunner(LanguageRunner):
    name = "java"
    # The public class MUST be named `Solution` (file name must match for javac).
    source_filename = "Solution.java"
    extra_memory_mb = 192  # metaspace, code cache, thread stacks live outside -Xmx
    check_rss_limit = False
    memory_error_markers = ("java.lang.OutOfMemoryError",)

    def compile_command(self) -> Optional[list[str]]:
        return ["javac", "-encoding", "UTF-8", self.source_filename]

    def run_command(self, memory_limit_kb: int) -> list[str]:
        heap_mb = max(16, memory_limit_kb // 1024)
        return [
            "java",
            f"-Xmx{heap_mb}m",
            "-XX:+UseSerialGC",  # fewer GC threads -> stays under pids_limit
            "-XX:-UsePerfData",  # no /tmp/hsperfdata mmap file
            "-XX:ActiveProcessorCount=1",
            "-Dfile.encoding=UTF-8",
            "Solution",
        ]


_RUNNERS: dict[str, LanguageRunner] = {
    runner.name: runner for runner in (PythonRunner(), CppRunner(), JavaRunner())
}
_ALIASES = {"python3": "python", "py": "python", "c++": "cpp", "cxx": "cpp"}

SUPPORTED_LANGUAGES = tuple(_RUNNERS)


def get_runner(language: str) -> LanguageRunner:
    key = _ALIASES.get(language.strip().lower(), language.strip().lower())
    try:
        return _RUNNERS[key]
    except KeyError:
        raise UnsupportedLanguageError(
            f"Unsupported language {language!r}. Supported: {', '.join(SUPPORTED_LANGUAGES)}"
        ) from None
