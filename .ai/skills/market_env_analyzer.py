"""
Market Environment Analyzer Skill
Usage: python .ai/skills/market_env_analyzer.py [date]
"""
import sys
import logging
import json
import datetime as dt
from ashare.core.db import MySQLWriter, DatabaseConfig
from ashare.indicators.weekly_env_builder import WeeklyEnvironmentBuilder

def analyze_market_env(target_date: str = None):
    # Setup basics
    logging.basicConfig(level=logging.WARNING)
    db = MySQLWriter(DatabaseConfig.from_env())
    
    # Resolve date
    if not target_date:
        target_date = dt.date.today().isoformat()
    
    print(f"Analyzing market environment for: {target_date}...")

    # Init builder
    builder = WeeklyEnvironmentBuilder(
        db_writer=db,
        logger=logging.getLogger("EnvAnalyzer"),
        index_codes=["sh.000001"],
        board_env_enabled=True,
        board_spot_enabled=True,
        env_index_score_threshold=0.5,
        weekly_soft_gate_strength_threshold=0.5
    )

    # Build context
    try:
        context = builder.build_environment_context(target_date)
    except Exception as e:
        print(f"Error building environment: {e}")
        return

    # Extract key metrics
    report = {
        "date": target_date,
        "regime": context.get("regime"),
        "risk_level": context.get("weekly_risk_level"),
        "gate_policy": context.get("weekly_gate_policy"),
        "weekly_phase": context.get("weekly_phase"),
        "position_hint": context.get("effective_position_hint"),
        "structure_status": context.get("weekly_structure_status"),
        "money_proxy": context.get("weekly_money_proxy"),
        "tags": context.get("weekly_tags"),
    }
    
    # Print readable report
    print("\n====== MARKET ENVIRONMENT REPORT ======")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    
    print("\n[Weekly Scenario Plan]")
    print(f"Plan A: {context.get('weekly_plan_a')}")
    print(f"Plan B: {context.get('weekly_plan_b')}")
    
    # Check boards summary if available
    boards = context.get("boards", {{}})
    if boards:
        strong_boards = [k for k, v in boards.items() if v.get("status") == "strong"]
        print(f"\n[Board Summary]")
        print(f"Total Boards: {len(boards)}")
        print(f"Strong Boards ({len(strong_boards)}): {', '.join(strong_boards[:10])}...")

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    analyze_market_env(target_date)