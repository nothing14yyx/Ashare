import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_FILE = OUTPUT_DIR / "project_for_llm.txt"

# 想导出的文件后缀（按需增减）
INCLUDE_EXT = {
    ".py",
    ".toml",
    ".md",
    ".txt",
    ".json",
    ".yml",
    ".yaml",
}

# 想忽略的目录（虚拟环境、git、缓存这些没必要给大模型看）
EXCLUDE_DIRS = {
    ".git",
    ".idea",
    "__pycache__",
    ".pytest_cache",
    "venv",
    ".venv",
    "dbn_trading_auto",
    "output",
}


def should_skip_dir(path: Path) -> bool:
    return any(part in EXCLUDE_DIRS for part in path.parts)


def main() -> None:
    files: list[Path] = []

    output_resolved = OUTPUT_FILE.resolve()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for root, dirs, filenames in os.walk(PROJECT_ROOT):
        root_path = Path(root)

        # 过滤不想要的目录
        if should_skip_dir(root_path.relative_to(PROJECT_ROOT)):
            # 清空 dirs，阻止继续往下走
            dirs[:] = []
            continue

        for name in filenames:
            p = root_path / name

            # ✅ 关键：跳过输出文件自身，防止“写的时候又读自己”导致内容重复
            if p.resolve() == output_resolved:
                continue

            if p.suffix.lower() in INCLUDE_EXT:
                files.append(p)

    files.sort()

    with OUTPUT_FILE.open("w", encoding="utf-8") as out:
        out.write(
            f"# Project dump for LLM\n"
            f"# Root: {PROJECT_ROOT}\n"
            f"# Total files: {len(files)}\n"
            f"\n"
        )

        for path in files:
            rel = path.relative_to(PROJECT_ROOT)
            out.write("\n")
            out.write("=" * 80 + "\n")
            out.write(f"FILE: {rel.as_posix()}\n")
            out.write("=" * 80 + "\n\n")

            try:
                with path.open("r", encoding="utf-8") as f:
                    out.write(f.read())
            except UnicodeDecodeError:
                out.write("# [SKIP] 非 UTF-8 文本文件，未导出内容。\n")

    print(f"导出完成，共 {len(files)} 个文件")
    print(f"输出文件: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
