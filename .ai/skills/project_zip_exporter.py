# tool/export_zip_project.py
# Python 3.13+
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

# === 与旧导出脚本保持一致的“导出范围” ===
INCLUDE_EXT = {
    ".py",
    ".toml",
    ".md",
    ".txt",
    ".json",
    ".yml",
    ".yaml",
}

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


@dataclass(frozen=True)
class FileEntry:
    rel_path: str
    size: int
    mtime: int
    sha256: str


def should_skip_dir(rel_dir: Path) -> bool:
    # 只要相对路径的任何一段命中 EXCLUDE_DIRS，就整段跳过（与旧脚本一致）
    return any(part in EXCLUDE_DIRS for part in rel_dir.parts)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def to_posix_relpath(base: Path, p: Path) -> str:
    return p.relative_to(base).as_posix()


def collect_files(project_root: Path, out_zip: Path) -> list[Path]:
    files: list[Path] = []
    project_root = project_root.resolve()
    out_zip_resolved = out_zip.resolve()

    for root, dirs, filenames in os.walk(project_root):
        root_path = Path(root)

        try:
            rel_dir = root_path.relative_to(project_root)
        except ValueError:
            # 理论上不应发生
            dirs[:] = []
            continue

        # 目录过滤：命中就阻止继续深入
        if rel_dir != Path(".") and should_skip_dir(rel_dir):
            dirs[:] = []
            continue

        # 同时把下一层 dirs 里命中的排除目录删掉（加速）
        kept_dirs: list[str] = []
        for d in dirs:
            if d in EXCLUDE_DIRS:
                continue
            kept_dirs.append(d)
        dirs[:] = kept_dirs

        for name in filenames:
            p = root_path / name

            # 跳过符号链接（避免不同机器解压差异）
            try:
                if p.is_symlink():
                    continue
                if not p.is_file():
                    continue
            except OSError:
                continue

            # 跳过输出 zip 自身（极端情况下同名覆盖/重复运行时可避免自包含）
            try:
                if p.resolve() == out_zip_resolved:
                    continue
            except OSError:
                pass

            # 只按后缀白名单导出（与旧脚本一致）
            if p.suffix.lower() not in INCLUDE_EXT:
                continue

            files.append(p)

    files.sort(key=lambda x: to_posix_relpath(project_root, x))
    return files


def zip_write_file(zf: ZipFile, project_root: Path, file_path: Path) -> FileEntry:
    rel_posix = to_posix_relpath(project_root, file_path)
    st = file_path.stat()
    mtime = int(st.st_mtime)
    size = int(st.st_size)
    digest = sha256_file(file_path)

    zi = ZipInfo(rel_posix)
    t = time.localtime(mtime)
    zi.date_time = (max(t.tm_year, 1980), t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)

    # 尝试保存权限位（在 *nix 更有意义，Windows 下也无害）
    try:
        zi.external_attr = (st.st_mode & 0xFFFF) << 16
    except Exception:
        pass

    # 二进制读取写入：不做任何编码/换行处理
    with file_path.open("rb") as f:
        data = f.read()
    zf.writestr(zi, data, compress_type=ZIP_DEFLATED)

    return FileEntry(rel_path=rel_posix, size=size, mtime=mtime, sha256=digest)


def build_manifest(entries: list[FileEntry], project_root: Path, out_zip: Path) -> dict:
    return {
        "tool": ".ai/skills/project_zip_exporter.py",
        "root": str(project_root.resolve()),
        "zip": str(out_zip.resolve()),}
# ... (lines skipped) ...
def main() -> int:
    script_dir = Path(__file__).resolve().parent      # .../.ai/skills
    project_root = script_dir.parents[1]              # 项目根
    output_dir = project_root / "output"     # output

    ts = datetime.now(ZoneInfo("Asia/Singapore")).strftime("%Y%m%d_%H%M%S")
    default_out_zip = output_dir / f"project_export_{ts}.zip"

    ap = argparse.ArgumentParser(
        description="Export project as zip (binary-preserved) with the same scope rules as export_project.py."
    )
    ap.add_argument("--src", type=str, default=str(project_root), help="Project root (default: project root).")
    ap.add_argument("--out", type=str, default=str(default_out_zip), help="Output zip path (default: output/...).")
    ap.add_argument("--compresslevel", type=int, default=6, help="Zip deflate compresslevel 0-9 (default: 6).")
    args = ap.parse_args()

    src = Path(args.src).resolve()
    if not src.exists() or not src.is_dir():
        print(f"[ERROR] --src is not a directory: {src}", file=sys.stderr)
        return 2

    out_zip = Path(args.out).resolve()
    out_zip.parent.mkdir(parents=True, exist_ok=True)

    files = collect_files(src, out_zip)
    entries: list[FileEntry] = []

    with ZipFile(
        out_zip,
        mode="w",
        compression=ZIP_DEFLATED,
        compresslevel=max(0, min(int(args.compresslevel), 9)),
    ) as zf:
        for p in files:
            entries.append(zip_write_file(zf, src, p))

        manifest = build_manifest(entries, src, out_zip)
        zf.writestr("__manifest__.json", json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))

    print(f"[OK] Wrote zip: {out_zip}")
    print(f"[OK] Files: {len(entries)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
