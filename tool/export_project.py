from __future__ import annotations

import argparse
import datetime as dt
import json
import zipfile
from pathlib import Path
from typing import Iterable, List, Set

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_EXCLUDE_NAMES = {
    ".git",
    ".idea",
    ".mypy_cache",
    ".pytest_cache",
    ".venv",
    "__pycache__",
    "data",
    "env",
    "output",
    "tool/output",
    "venv",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _should_exclude(path: Path, root: Path, exclude_names: Set[str], include_hidden: bool) -> bool:
    rel_parts = path.relative_to(root).parts
    if not include_hidden and any(part.startswith(".") for part in rel_parts):
        return True
    return any(part in exclude_names for part in rel_parts)


def _iter_project_files(root: Path, exclude_names: Set[str], include_hidden: bool) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if _should_exclude(path, root, exclude_names, include_hidden):
            continue
        yield path


def export_project(outdir: Path, include_hidden: bool, extra_excludes: List[str]) -> tuple[Path, Path]:
    root = _project_root()
    excludes = set(DEFAULT_EXCLUDE_NAMES)
    excludes.update(extra_excludes)

    outdir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_path = outdir / f"project_snapshot_{ts}.zip"
    manifest_path = outdir / f"project_snapshot_{ts}.json"

    files = sorted(_iter_project_files(root, excludes, include_hidden), key=lambda p: str(p.relative_to(root)))

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            zf.write(f, arcname=str(f.relative_to(root)))

    manifest = {
        "exported_at": dt.datetime.now().isoformat(timespec="seconds"),
        "project_root": str(root),
        "out_zip": str(zip_path),
        "included_files": [str(f.relative_to(root)) for f in files],
        "excluded_names": sorted(excludes),
        "include_hidden": include_hidden,
    }

    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print("==== Project Export ====")
    print(f"root: {root}")
    print(f"outdir: {outdir}")
    print(f"zip: {zip_path.name}")
    print(f"files: {len(files)}")
    print("========================")

    return zip_path, manifest_path


def main() -> None:
    parser = argparse.ArgumentParser(description="导出项目源码（压缩包 + manifest），便于调试与分享。")
    parser.add_argument(
        "--outdir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="输出目录（默认：tool/output）",
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="是否包含隐藏文件（默认不包含）",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="额外排除的目录或文件名（可多次指定）",
    )

    args = parser.parse_args()
    outdir = Path(args.outdir) if args.outdir else DEFAULT_OUTPUT_DIR
    extra_excludes = [str(Path(p).name) for p in args.exclude if p]

    zip_path, manifest_path = export_project(outdir, include_hidden=args.include_hidden, extra_excludes=extra_excludes)

    print("\n✅ 导出完成")
    print(f"- Zip:      {zip_path}")
    print(f"- Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
