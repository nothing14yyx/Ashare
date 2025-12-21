# export_open_monitor_bundle.py
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---- 兼容你的项目：优先用项目自带的 DB 配置/连接工具；失败再用本地 env DSN ----
def _try_make_engine() -> Engine:
    # 1) 尝试使用你的项目封装
    try:
        from ashare.db import DatabaseConfig, MySQLWriter  # type: ignore
        try:
            cfg = DatabaseConfig.from_env()  # 你之前有这个
        except Exception:
            # 你说“缺少环境变量，用 config 的”
            from ashare.config import get_section  # type: ignore

            # 兼容多种 section 名
            for sec_name in ("mysql", "db", "database"):
                try:
                    sec = get_section(sec_name)
                except Exception:
                    sec = None
                if sec:
                    break
            if not sec:
                raise RuntimeError("无法从 config 读取 DB 配置：请检查 ashare.config.get_section() 的 section 名")

            # 尽量兼容字段名
            host = sec.get("host") or sec.get("hostname") or "127.0.0.1"
            port = int(sec.get("port") or 3306)
            user = sec.get("user") or sec.get("username")
            password = sec.get("password") or sec.get("pass") or ""
            db_name = sec.get("db_name") or sec.get("database") or sec.get("db")
            charset = sec.get("charset") or "utf8mb4"

            if not user or not db_name:
                raise RuntimeError(f"config 里缺少 user/db_name：{sec}")

            url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}?charset={charset}"
            return create_engine(url, pool_pre_ping=True)

        return MySQLWriter(cfg).engine
    except Exception:
        pass

    # 2) 兜底：用环境变量 DB_URL（你如果愿意也可以设置）
    db_url = os.environ.get("DB_URL")
    if not db_url:
        raise RuntimeError(
            "无法创建数据库连接：\n"
            " - 你的项目连接工具导入失败，且未设置 DB_URL。\n"
            "解决：优先确保 ashare.db / ashare.config 可用；或临时设置 DB_URL。"
        )
    return create_engine(db_url, pool_pre_ping=True)


def _safe_filename(s: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _fetch_one(conn, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
    return conn.execute(text(sql), params or {}).fetchone()


def _fetch_all(conn, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Tuple[Any, ...]]:
    return conn.execute(text(sql), params or {}).fetchall()


def _table_exists(conn, table: str, db_name: Optional[str] = None) -> bool:
    if db_name:
        row = _fetch_one(
            conn,
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema=:db AND table_name=:t
            """,
            {"db": db_name, "t": table},
        )
        return bool(row[0]) if row else False
    # fallback：直接试
    try:
        _fetch_one(conn, f"SELECT 1 FROM `{table}` LIMIT 1")
        return True
    except Exception:
        return False


def _get_db_name(conn) -> Optional[str]:
    try:
        row = _fetch_one(conn, "SELECT DATABASE()")
        return row[0] if row else None
    except Exception:
        return None


def _show_create_table(conn, table: str) -> str:
    row = _fetch_one(conn, f"SHOW CREATE TABLE `{table}`")
    # MySQL 返回 (table_name, create_sql)
    if not row or len(row) < 2:
        return f"-- SHOW CREATE TABLE `{table}` 返回空\n"
    return str(row[1]) + ";\n"


def _select_existing_cols(conn, table: str, wanted: Sequence[str], db_name: Optional[str]) -> List[str]:
    if not db_name:
        # 粗暴兜底：不查 columns，直接返回 wanted（后续 SELECT 可能报错）
        return list(wanted)

    rows = _fetch_all(
        conn,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=:db AND table_name=:t
        """,
        {"db": db_name, "t": table},
    )
    existing = {r[0] for r in rows}
    return [c for c in wanted if c in existing]


def _dump_query_to_csv(conn, out_dir: Path, name: str, sql: str, params: Dict[str, Any]) -> Path:
    df = pd.read_sql(text(sql), conn, params=params)
    p = out_dir / f"{_safe_filename(name)}.csv"
    df.to_csv(p, index=False, encoding="utf-8-sig")
    return p


def _zip_dir(folder: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in folder.rglob("*"):
            if p.is_file():
                z.write(p, arcname=p.relative_to(folder))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--monitor-date", default="2025-12-21")
    ap.add_argument("--run-id", default="POSTCLOSE")
    ap.add_argument("--asof-date", default="2025-12-19")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    monitor_date = args.monitor_date
    run_id = args.run_id
    asof_date = args.asof_date

    engine = _try_make_engine()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    base = Path(args.out) if args.out else Path(f"open_monitor_bundle_{monitor_date}_{run_id}_{stamp}")
    out_dir = base
    out_dir.mkdir(parents=True, exist_ok=True)

    with engine.connect() as conn:
        db_name = _get_db_name(conn)

        meta = {
            "monitor_date": monitor_date,
            "run_id": run_id,
            "asof_date": asof_date,
            "db_name": db_name,
            "exported_at": dt.datetime.now().isoformat(sep=" ", timespec="seconds"),
        }
        _write_text(out_dir / "meta.json", json.dumps(meta, ensure_ascii=False, indent=2))

        tables = [
            "strategy_open_monitor_eval",
            "strategy_open_monitor_quote",
            "strategy_open_monitor_env",
            "strategy_env_index_snapshot",
        ]

        # 1) 表结构
        schema_dir = out_dir / "schema"
        schema_sql = []
        for t in tables:
            if _table_exists(conn, t, db_name):
                schema_sql.append(f"-- {t}\n{_show_create_table(conn, t)}\n")
            else:
                schema_sql.append(f"-- {t} NOT FOUND\n")
        _write_text(schema_dir / "tables.sql", "\n".join(schema_sql))

        # 2) 统计 + 明细（尽量兼容你字段名）
        data_dir = out_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        # eval：优先抽你关心字段，避免字段名不同就爆炸
        eval_table = "strategy_open_monitor_eval"
        if _table_exists(conn, eval_table, db_name):
            wanted = [
                "monitor_date", "run_id", "code", "name",
                "status", "action", "gate_action",
                "candidate_status", "status_reason",
                "sig_date", "sig_close",
            ]
            cols = _select_existing_cols(conn, eval_table, wanted, db_name)
            sel = ", ".join(f"`{c}`" for c in cols) if cols else "*"
            _dump_query_to_csv(
                conn,
                data_dir,
                "eval_rows",
                f"""
                SELECT {sel}
                FROM `{eval_table}`
                WHERE monitor_date=:monitor_date AND run_id=:run_id
                ORDER BY code
                """,
                {"monitor_date": monitor_date, "run_id": run_id},
            )

            # 分布统计：分别试 status/action/gate_action（存在才跑）
            for field in ("status", "action", "gate_action"):
                cols_exist = _select_existing_cols(conn, eval_table, [field], db_name)
                if cols_exist:
                    _dump_query_to_csv(
                        conn,
                        data_dir,
                        f"eval_groupby_{field}",
                        f"""
                        SELECT `{field}` AS value, COUNT(*) AS cnt
                        FROM `{eval_table}`
                        WHERE monitor_date=:monitor_date AND run_id=:run_id
                        GROUP BY `{field}`
                        ORDER BY cnt DESC
                        """,
                        {"monitor_date": monitor_date, "run_id": run_id},
                    )

        # quote：检查 live_trade_date/asof_trade_date（你现在周日很可疑）
        quote_table = "strategy_open_monitor_quote"
        if _table_exists(conn, quote_table, db_name):
            wanted = [
                "monitor_date", "run_id", "code",
                "asof_trade_date", "live_trade_date",
                "live_open", "live_latest",
                "volume", "amount", "vol_ratio",
                "sig_close",
            ]
            cols = _select_existing_cols(conn, quote_table, wanted, db_name)
            sel = ", ".join(f"`{c}`" for c in cols) if cols else "*"
            _dump_query_to_csv(
                conn,
                data_dir,
                "quote_rows",
                f"""
                SELECT {sel}
                FROM `{quote_table}`
                WHERE monitor_date=:monitor_date AND run_id=:run_id
                ORDER BY code
                """,
                {"monitor_date": monitor_date, "run_id": run_id},
            )
            # min/max
            cols_minmax = _select_existing_cols(conn, quote_table, ["live_trade_date", "asof_trade_date"], db_name)
            if len(cols_minmax) == 2:
                _dump_query_to_csv(
                    conn,
                    data_dir,
                    "quote_minmax",
                    f"""
                    SELECT
                      run_id,
                      COUNT(*) AS cnt,
                      MIN(live_trade_date) AS min_live,
                      MAX(live_trade_date) AS max_live,
                      MIN(asof_trade_date) AS min_asof,
                      MAX(asof_trade_date) AS max_asof
                    FROM `{quote_table}`
                    WHERE monitor_date=:monitor_date AND run_id=:run_id
                    """,
                    {"monitor_date": monitor_date, "run_id": run_id},
                )

        # env：把本次的环境快照导出
        env_table = "strategy_open_monitor_env"
        if _table_exists(conn, env_table, db_name):
            _dump_query_to_csv(
                conn,
                data_dir,
                "env_rows",
                f"""
                SELECT *
                FROM `{env_table}`
                WHERE monitor_date=:monitor_date AND run_id=:run_id
                """,
                {"monitor_date": monitor_date, "run_id": run_id},
            )

        # 指数快照：按 asof_date 拉最近几条
        idx_table = "strategy_env_index_snapshot"
        if _table_exists(conn, idx_table, db_name):
            cols = _select_existing_cols(conn, idx_table, ["asof_date"], db_name)
            if cols:
                _dump_query_to_csv(
                    conn,
                    data_dir,
                    "index_snapshot_recent",
                    f"""
                    SELECT *
                    FROM `{idx_table}`
                    WHERE asof_date=:asof_date
                    ORDER BY checked_at DESC
                    LIMIT 50
                    """,
                    {"asof_date": asof_date},
                )
            else:
                # 没 asof_date 就粗暴拉 50
                _dump_query_to_csv(
                    conn,
                    data_dir,
                    "index_snapshot_recent",
                    f"""
                    SELECT *
                    FROM `{idx_table}`
                    ORDER BY checked_at DESC
                    LIMIT 50
                    """,
                    {},
                )

    # 3) 打包
    zip_path = out_dir.with_suffix(".zip")
    _zip_dir(out_dir, zip_path)

    print(f"[OK] bundle dir: {out_dir}")
    print(f"[OK] zip file  : {zip_path}")


if __name__ == "__main__":
    main()
