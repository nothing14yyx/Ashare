"""
export_db_snapshot.py

导出 MySQL 数据库快照（表清单 / 字段 / 行数 / 样例数据 / 可选日期范围），用于给 LLM 理解你当前已有的数据。

特点：
- 默认从项目根目录 config.yaml 读取数据库配置（顶层 database.*）
- 也支持通过环境变量 ASHARE_CONFIG_FILE 指定配置文件路径
- 生成两份文件：
  1) tool/output/db_snapshot_YYYYMMDD_HHMMSS.md
  2) tool/output/db_snapshot_YYYYMMDD_HHMMSS.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    import yaml  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "缺少依赖 pyyaml，无法读取 config.yaml。请先安装：pip install pyyaml"
    ) from e


# ----------------------------
# Config / DB Connect
# ----------------------------

@dataclass
class DBConfig:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    db_name: str = "ashare"

    def sqlalchemy_url(self) -> str:
        pwd = quote_plus(self.password) if self.password else ""
        cred = self.user if not pwd else f"{self.user}:{pwd}"
        # charset 兼容中文字段/注释；pool_pre_ping 提高连接稳定性
        return f"mysql+pymysql://{cred}@{self.host}:{self.port}/{self.db_name}?charset=utf8mb4"


def _project_root() -> Path:
    # .../AShare/.ai/skills/db_snapshot_exporter.py -> parents[2] == .../AShare
    return Path(__file__).resolve().parents[2]


def _default_config_path() -> Path:
    root = _project_root()
    # 优先 config.yaml，其次 config.yml
    p1 = root / "config.yaml"
    p2 = root / "config.yml"
    return p1 if p1.exists() else p2
# ... (lines skipped) ...
    parser.add_argument(
        "--outdir",
        type=str,
        default=str(_project_root() / "tool" / "output"),
        help="输出目录（默认：tool/output）",
    )


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"未找到配置文件：{path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"配置文件格式不正确（顶层不是 dict）：{path}")
    return data


def _load_db_config_from_yaml(cfg: Dict[str, Any]) -> DBConfig:
    section = cfg.get("database") or {}
    if not isinstance(section, dict):
        section = {}

    host = os.getenv("MYSQL_HOST") or section.get("host") or "127.0.0.1"
    port_raw = os.getenv("MYSQL_PORT") or section.get("port") or 3306
    user = os.getenv("MYSQL_USER") or section.get("user") or "root"
    password = os.getenv("MYSQL_PASSWORD")
    if password is None:
        password = section.get("password") or ""
    db_name = os.getenv("MYSQL_DB_NAME") or section.get("db_name") or "ashare"

    try:
        port = int(port_raw)
    except Exception:
        port = 3306

    return DBConfig(host=str(host), port=port, user=str(user), password=str(password), db_name=str(db_name))


def _connect_engine(config_path: Optional[Path] = None) -> Tuple[Engine, Path, DBConfig]:
    # 1) 明确传入 --config
    # 2) 环境变量 ASHARE_CONFIG_FILE
    # 3) 项目根目录 config.yaml / config.yml
    env_cfg = os.environ.get("ASHARE_CONFIG_FILE")
    cfg_path = config_path or (Path(env_cfg) if env_cfg else _default_config_path())

    cfg = _load_yaml(cfg_path)
    db_cfg = _load_db_config_from_yaml(cfg)

    url = db_cfg.sqlalchemy_url()
    engine = create_engine(url, future=True, pool_pre_ping=True)

    # quick ping
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    return engine, cfg_path, db_cfg


# ----------------------------
# Snapshot helpers
# ----------------------------

_DATE_CANDIDATE_COLS = [
    "trade_date",
    "date",
    "dt",
    "day",
    "period",
    "statDate",
    "pubDate",
    "publish_date",
    "datetime",
]


def _fetch_tables(engine: Engine, db_name: str) -> List[Dict[str, Any]]:
    """
    使用 information_schema.tables 获取表/视图清单与近似行数、注释。
    """
    sql = text(
        """
        SELECT
            TABLE_NAME AS table_name,
            TABLE_TYPE AS table_type,
            IFNULL(TABLE_ROWS, 0) AS approx_rows,
            IFNULL(TABLE_COMMENT, '') AS table_comment
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = :db
        ORDER BY TABLE_NAME
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"db": db_name})
    return df.to_dict(orient="records")


def _fetch_columns(engine: Engine, db_name: str, table: str) -> List[Dict[str, Any]]:
    sql = text(
        """
        SELECT
            COLUMN_NAME AS column_name,
            COLUMN_TYPE AS column_type,
            IS_NULLABLE AS is_nullable,
            COLUMN_DEFAULT AS column_default,
            IFNULL(COLUMN_COMMENT, '') AS column_comment
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl
        ORDER BY ORDINAL_POSITION
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"db": db_name, "tbl": table})
    return df.to_dict(orient="records")


def _safe_quote_ident(name: str) -> str:
    # MySQL 反引号转义
    return "`" + name.replace("`", "``") + "`"


def _estimate_row_count(engine: Engine, db_name: str, table: str) -> int:
    # 近似行数已经在 tables 里拿到；这里提供一个兜底的精确 count（可能慢）
    sql = text(f"SELECT COUNT(1) AS cnt FROM {_safe_quote_ident(table)}")
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().first()
    return int(row["cnt"]) if row and row.get("cnt") is not None else 0


def _pick_date_column(columns: List[Dict[str, Any]]) -> Optional[str]:
    col_names = [str(c.get("column_name", "")) for c in columns]
    for cand in _DATE_CANDIDATE_COLS:
        if cand in col_names:
            return cand
    # 再做一次宽松匹配（例如 tradeDate / TRADE_DATE）
    lower_map = {c.lower(): c for c in col_names}
    for cand in _DATE_CANDIDATE_COLS:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def _fetch_date_range(engine: Engine, table: str, date_col: str) -> Tuple[Optional[str], Optional[str]]:
    sql = text(
        f"SELECT MIN({_safe_quote_ident(date_col)}) AS mn, MAX({_safe_quote_ident(date_col)}) AS mx "
        f"FROM {_safe_quote_ident(table)}"
    )
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().first()

    def _to_str(v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, (dt.date, dt.datetime)):
            return v.isoformat()
        return str(v)

    mn = _to_str(row["mn"]) if row else None
    mx = _to_str(row["mx"]) if row else None
    return mn, mx


def _fetch_sample(engine: Engine, table: str, limit: int) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []
    sql = text(f"SELECT * FROM {_safe_quote_ident(table)} LIMIT :n")
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"n": int(limit)})

    # 统一转成可 JSON 化
    records: List[Dict[str, Any]] = []
    for rec in df.to_dict(orient="records"):
        clean: Dict[str, Any] = {}
        for k, v in rec.items():
            if isinstance(v, (dt.datetime, dt.date)):
                clean[k] = v.isoformat()
            elif pd.isna(v):
                clean[k] = None
            else:
                clean[k] = v
        records.append(clean)
    return records


# ----------------------------
# Export
# ----------------------------

def _write_markdown(out_path: Path, meta: Dict[str, Any], tables: List[Dict[str, Any]]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write("# Database Snapshot for LLM\n\n")
        f.write(f"- exported_at: {meta.get('exported_at')}\n")
        f.write(f"- config_path: {meta.get('config_path')}\n")
        f.write(f"- db: {meta.get('db_name')}\n")
        f.write(f"- host: {meta.get('host')}:{meta.get('port')}\n")
        f.write(f"- user: {meta.get('user')}\n")
        f.write(f"- tables: {len(tables)}\n\n")

        f.write("## Tables\n\n")
        f.write("| table | type | approx_rows | date_col | date_min | date_max | comment |\n")
        f.write("|---|---:|---:|---|---|---|---|\n")
        for t in tables:
            f.write(
                f"| {t.get('table_name')} | {t.get('table_type')} | {t.get('approx_rows')} | "
                f"{t.get('date_col') or ''} | {t.get('date_min') or ''} | {t.get('date_max') or ''} | "
                f"{(t.get('table_comment') or '').replace('|',' ')} |\n"
            )

        f.write("\n---\n\n")
        for t in tables:
            f.write(f"## {t.get('table_name')}\n\n")
            f.write(f"- type: {t.get('table_type')}\n")
            f.write(f"- approx_rows: {t.get('approx_rows')}\n")
            if t.get("exact_rows") is not None:
                f.write(f"- exact_rows: {t.get('exact_rows')}\n")
            if t.get("table_comment"):
                f.write(f"- comment: {t.get('table_comment')}\n")
            if t.get("date_col"):
                f.write(f"- date_col: {t.get('date_col')}\n")
                f.write(f"- date_min: {t.get('date_min')}\n")
                f.write(f"- date_max: {t.get('date_max')}\n")

            f.write("\n### Columns\n\n")
            f.write("| name | type | nullable | default | comment |\n")
            f.write("|---|---|---:|---|---|\n")
            for c in t.get("columns", []):
                f.write(
                    f"| {c.get('column_name')} | {c.get('column_type')} | {c.get('is_nullable')} | "
                    f"{c.get('column_default') if c.get('column_default') is not None else ''} | "
                    f"{(c.get('column_comment') or '').replace('|',' ')} |\n"
                )

            samples = t.get("sample_rows") or []
            if samples:
                f.write("\n### Sample Rows\n\n")
                f.write("```json\n")
                f.write(json.dumps(samples, ensure_ascii=False, indent=2))
                f.write("\n```\n")

            f.write("\n---\n\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export MySQL database snapshot for LLM.")
    parser.add_argument(
        "--config",
        type=str,
        default="",
        help="配置文件路径（默认：环境变量 ASHARE_CONFIG_FILE；否则项目根目录 config.yaml/config.yml）",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default=str(Path(__file__).resolve().parent / "output"),
        help="输出目录（默认：tool/output）",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=5,
        help="每张表导出多少行样例（默认：5；设为 0 则不导出样例）",
    )
    parser.add_argument(
        "--max-tables",
        type=int,
        default=0,
        help="最多导出多少张表（0 表示全部）",
    )
    parser.add_argument(
        "--exact-counts",
        action="store_true",
        help="对每张表执行 SELECT COUNT(1) 获取精确行数（可能很慢，不建议大表开启）",
    )
    parser.add_argument(
        "--no-date-range",
        action="store_true",
        help="不计算 MIN/MAX 日期范围（更快）",
    )

    args = parser.parse_args()
    config_path = Path(args.config) if args.config else None

    engine, used_cfg_path, db_cfg = _connect_engine(config_path=config_path)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir)
    md_path = outdir / f"db_snapshot_{ts}.md"
    json_path = outdir / f"db_snapshot_{ts}.json"

    meta: Dict[str, Any] = {
        "exported_at": dt.datetime.now().isoformat(timespec="seconds"),
        "config_path": str(used_cfg_path),
        "db_name": db_cfg.db_name,
        "host": db_cfg.host,
        "port": db_cfg.port,
        "user": db_cfg.user,
    }

    print("==== DB Snapshot Export ====")
    print(f"config: {used_cfg_path}")
    print(f"db: {db_cfg.user}@{db_cfg.host}:{db_cfg.port}/{db_cfg.db_name}")
    print(f"outdir: {outdir.resolve()}")
    print("============================")

    tables = _fetch_tables(engine, db_cfg.db_name)

    if args.max_tables and args.max_tables > 0:
        tables = tables[: int(args.max_tables)]

    enriched: List[Dict[str, Any]] = []
    for i, t in enumerate(tables, start=1):
        table_name = str(t.get("table_name"))
        table_type = str(t.get("table_type"))
        approx_rows = int(t.get("approx_rows") or 0)

        print(f"[{i}/{len(tables)}] {table_name} ({table_type}) ...")

        columns = _fetch_columns(engine, db_cfg.db_name, table_name)

        date_col = _pick_date_column(columns)
        date_min = None
        date_max = None
        if (not args.no_date_range) and date_col:
            try:
                date_min, date_max = _fetch_date_range(engine, table_name, date_col)
            except Exception as e:
                # 不中断导出
                date_min, date_max = None, None
                print(f"  [WARN] date range failed: {e}")

        exact_rows = None
        if args.exact_counts:
            try:
                exact_rows = _estimate_row_count(engine, db_cfg.db_name, table_name)
            except Exception as e:
                exact_rows = None
                print(f"  [WARN] exact count failed: {e}")

        sample_rows: List[Dict[str, Any]] = []
        if args.sample_rows and int(args.sample_rows) > 0:
            try:
                sample_rows = _fetch_sample(engine, table_name, int(args.sample_rows))
            except Exception as e:
                sample_rows = []
                print(f"  [WARN] sample fetch failed: {e}")

        enriched.append(
            {
                "table_name": table_name,
                "table_type": table_type,
                "approx_rows": approx_rows,
                "exact_rows": exact_rows,
                "table_comment": t.get("table_comment") or "",
                "columns": columns,
                "date_col": date_col,
                "date_min": date_min,
                "date_max": date_max,
                "sample_rows": sample_rows,
            }
        )

    outdir.mkdir(parents=True, exist_ok=True)

    # JSON
    with json_path.open("w", encoding="utf-8") as f:
        json.dump({"meta": meta, "tables": enriched}, f, ensure_ascii=False, indent=2)

    # Markdown
    _write_markdown(md_path, meta, enriched)

    print("\nExport completed.")
    print(f"- Markdown: {md_path}")
    print(f"- JSON:     {json_path}")


if __name__ == "__main__":
    main()
