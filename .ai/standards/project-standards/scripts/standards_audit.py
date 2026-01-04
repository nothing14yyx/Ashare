#!/usr/bin/env python
import argparse
import ast
import re
import sys
from pathlib import Path


DEFAULT_EXCLUDE_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".tox",
    "build",
    "dist",
}

DEFAULT_FORBID_PATTERNS = [
    r"password\s*=",
    r"passwd\s*=",
    r"secret\s*=",
    r"api[_-]?key\s*=",
    r"(mysql|postgres)://",
]


def iter_py_files(root: Path, exclude_dirs):
    for path in root.rglob("*.py"):
        if any(part in exclude_dirs for part in path.parts):
            continue
        yield path


def load_lines(path: Path):
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="latin-1")
    return text.splitlines()


def check_sizes(path: Path, lines, max_file_lines, max_func_lines, max_class_lines):
    issues = []
    if len(lines) > max_file_lines:
        issues.append((path, 1, f"File exceeds {max_file_lines} lines ({len(lines)})"))

    try:
        tree = ast.parse("\n".join(lines), filename=str(path))
    except SyntaxError as exc:
        issues.append((path, exc.lineno or 1, f"Syntax error: {exc.msg}"))
        return issues

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end = getattr(node, "end_lineno", None)
            if end is None:
                continue
            size = end - node.lineno + 1
            if size > max_func_lines:
                issues.append(
                    (path, node.lineno, f"Function '{node.name}' exceeds {max_func_lines} lines ({size})")
                )
        elif isinstance(node, ast.ClassDef):
            end = getattr(node, "end_lineno", None)
            if end is None:
                continue
            size = end - node.lineno + 1
            if size > max_class_lines:
                issues.append(
                    (path, node.lineno, f"Class '{node.name}' exceeds {max_class_lines} lines ({size})")
                )
    return issues


def check_imports(path: Path, lines, forbid_imports):
    issues = []
    try:
        tree = ast.parse("\n".join(lines), filename=str(path))
    except SyntaxError:
        return issues

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.names:
            if any(alias.name == "*" for alias in node.names):
                issues.append((path, node.lineno, "Star import used"))
            module = node.module or ""
            if module in forbid_imports:
                issues.append((path, node.lineno, f"Forbidden import: {module}"))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in forbid_imports:
                    issues.append((path, node.lineno, f"Forbidden import: {alias.name}"))
    return issues


def check_patterns(path: Path, lines, patterns):
    issues = []
    compiled = [re.compile(pat, re.IGNORECASE) for pat in patterns]
    for idx, line in enumerate(lines, start=1):
        for pat in compiled:
            if pat.search(line):
                issues.append((path, idx, f"Forbidden pattern match: {pat.pattern}"))
                break
    return issues


def parse_args():
    parser = argparse.ArgumentParser(description="Audit Python codebase against basic standards.")
    parser.add_argument("--root", default=".", help="Root directory to scan.")
    parser.add_argument("--max-file-lines", type=int, default=400)
    parser.add_argument("--max-function-lines", type=int, default=60)
    parser.add_argument("--max-class-lines", type=int, default=200)
    parser.add_argument("--exclude-dirs", default=",".join(sorted(DEFAULT_EXCLUDE_DIRS)))
    parser.add_argument("--forbid-imports", action="append", default=[])
    parser.add_argument("--forbid-patterns", action="append", default=[])
    parser.add_argument("--warn-only", action="store_true", help="Exit 0 even if issues are found.")
    return parser.parse_args()


def main():
    args = parse_args()
    root = Path(args.root).resolve()
    exclude_dirs = {part.strip() for part in args.exclude_dirs.split(",") if part.strip()}
    forbid_imports = set(args.forbid_imports)
    patterns = DEFAULT_FORBID_PATTERNS + list(args.forbid_patterns)

    issues = []
    for path in iter_py_files(root, exclude_dirs):
        lines = load_lines(path)
        issues.extend(check_sizes(path, lines, args.max_file_lines, args.max_function_lines, args.max_class_lines))
        issues.extend(check_imports(path, lines, forbid_imports))
        issues.extend(check_patterns(path, lines, patterns))

    if issues:
        print(f"Found {len(issues)} issue(s):")
        for path, line, msg in issues:
            print(f"{path}:{line}: {msg}")
        if not args.warn_only:
            return 1
    else:
        print("No issues found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
