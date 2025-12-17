"""输出指数周线通道情景（线性回归通道 + 30/60 周均线）。

用法：
  python run_index_weekly_channel.py

说明：
  - 从数据库 history_index_daily_kline 读取配置里的指数日线数据
  - 聚合成周线后，输出当前周的通道情景（state/note/关键价位）
  - 该脚本不写库，只用于你手动核对与调参
"""

from __future__ import annotations

import json

import pandas as pd
from sqlalchemy import bindparam, text

from ashare.config import get_section
from ashare.db import DatabaseConfig, MySQLWriter
from ashare.weekly_channel_regime import WeeklyChannelClassifier


def main() -> None:
    db = MySQLWriter(DatabaseConfig.from_env())
    app_cfg = get_section("app") or {}
    codes = []
    if isinstance(app_cfg, dict):
        raw = app_cfg.get("index_codes", [])
        if isinstance(raw, (list, tuple)):
            codes = [str(c).strip() for c in raw if str(c).strip()]

    if not codes:
        print("config.yaml 未配置 app.index_codes，已跳过。")
        return

    stmt = (
        text(
            """
            SELECT `code`, `date`, `open`, `high`, `low`, `close`, `volume`, `amount`
            FROM history_index_daily_kline
            WHERE `code` IN :codes
            ORDER BY `code`, `date`
            """
        )
        .bindparams(bindparam("codes", expanding=True))
    )

    with db.engine.begin() as conn:
        df = pd.read_sql_query(stmt, conn, params={"codes": codes})

    if df.empty:
        print("history_index_daily_kline 为空或未找到指定指数。")
        return

    classifier = WeeklyChannelClassifier(primary_code="sh.000001")
    result = classifier.classify(df).to_payload()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
