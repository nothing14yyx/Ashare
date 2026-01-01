# AShare Project Context for AI Agents

> **Core Instruction**: Please respond to the user in **Chinese (Simplified)** unless explicitly requested otherwise. Keep code comments and commit messages in English or Chinese as per existing file conventions.

This document serves as the **Single Source of Truth** for all AI Agents interacting with the AShare quantitative analysis system.

---

## 1. Project Overview & Architecture

**AShare** is a Python-based quantitative trading system for the Chinese A-Share market, integrating data collection, strategy execution, and real-time monitoring.

### Directory Structure
- **`.ai/`**: AI Agent resources.
    - **`skills/`**: Custom executable scripts (e.g., `raw_reader.py`).
    - **`standards/`**: Project coding standards.
    - **`SKILLS.md`**: Documentation for AI skills.
- **`ashare/`**: Core source code.
    - **`core/`**: Infra (DB, Config, Schema).
    - **`data/`**: Data fetching (Baostock/AkShare).
    - **`strategies/`**: Trading strategies (MA5/MA20, Chip Filter).
    - **`monitor/`**: Real-time monitoring (Open Monitor).
    - **`indicators/`**: Market environment & technical indicators.
- **`scripts/`**: Executable workflows (`run_*.py`).
- **`tool/`**: Utilities & Ad-hoc scripts.
- **`config.yaml`**: Main configuration.

---

## 2. Business Logic & Strategies (策略逻辑)

### MA5-MA20 Trend Strategy (`ma5_ma20_trend_strategy`)
A trend-following strategy based on moving average crossovers.
1.  **Trend Filter**: Long-term bullish arrangement (`close > MA60 > MA250` & `MA20 > MA60`).
2.  **Entry Signals**:
    - **Golden Cross**: MA5 crosses above MA20 + Volume surge + MACD confirmation.
    - **Pullback**: Price pulls back to MA20 + MA5 trending up.
    - **W-Bottom**: W-pattern breakout confirmation.
3.  **Exit Signals**:
    - **Dead Cross**: MA5 crosses below MA20.
    - **Trend Break**: Price drops below MA20 with high volume.

### Open Monitor System (`open_monitor`)
Real-time execution checks for signals generated on the previous trading day.
1.  **Input**: Load `BUY` signals from `strategy_signal_events` (previous day).
2.  **Live Filter**: Fetch real-time quotes (Snapshot).
3.  **Risk Evaluation**:
    - **Environment Gate**: Checks market regime (Risk On/Off) & weekly channel status.
    - **Gap Risk**: excessive gap up (>5%) or gap down (<-3%).
    - **Limit Up**: Rejects if the stock is already at the limit up price.
4.  **Output**: Action decision (`EXECUTE`, `WAIT`, `STOP`).

---

## 3. Development Standards (开发规范)

- **Coding Style**: PEP 8. 4-space indentation.
- **Naming**: `snake_case` for functions/variables, `CamelCase` for classes.
- **Imports**: Use absolute imports (e.g., `from ashare.core.db import ...`).
- **Configuration**: Never hardcode credentials. Use `config.yaml`.
- **Testing**: Run `pytest` for unit tests. DB-related tests require a local MySQL instance.

---

## 4. Operations & Tools (操作指南)

### AI Agent Skills (Custom Tools)
The agent is equipped with custom tools in `.ai/skills/` to handle specific tasks.

#### **Raw Data Reader (`raw_reader`)**
*   **Purpose**: Read local data files (JSON, CSV, DB exports) located in `.gitignore` paths like `tool/output/`.
*   **Commands**:
    *   Read File: `python .ai/skills/raw_reader.py read <path>`
    *   List Dir: `python .ai/skills/raw_reader.py list <path>`

#### **Database Query (`db_query`)**
*   **Purpose**: Directly query the MySQL database to verify data or diagnose issues.
*   **Command**: `python .ai/skills/db_query.py "<SQL_QUERY>"`
*   **Note**: The script connects using `ashare.core.db`. `SELECT` queries limit to 20 rows by default unless specified.

#### **Database Snapshot Exporter (`db_snapshot_exporter`)**
*   **Purpose**: Export database schema/row counts/sample data into Markdown/JSON for analysis.
*   **Command**: `python .ai/skills/db_snapshot_exporter.py`
*   **Output**: `tool/output/db_snapshot_YYYYMMDD_HHMMSS.md` and `.json`

#### **Market Environment Analyzer (`market_env_analyzer`)**
*   **Purpose**: Build and print a weekly market environment report for a target date.
*   **Command**: `python .ai/skills/market_env_analyzer.py [YYYY-MM-DD]`

#### **Sector Rotation Analyzer (`sector_rotation_analyzer`)**
*   **Purpose**: Analyze sector rotation based on recent board industry history.
*   **Command**: `python .ai/skills/sector_rotation_analyzer.py [YYYY-MM-DD]`

#### **Network Tester - AkShare (`network_tester_akshare`)**
*   **Purpose**: Test AkShare and Eastmoney push2 connectivity and latency.
*   **Command**: `python .ai/skills/network_tester_akshare.py [options]`

#### **Network Tester - Baostock (`network_tester_baostock`)**
*   **Purpose**: Test Baostock core API availability and latency.
*   **Command**: `python .ai/skills/network_tester_baostock.py [options]`

#### **Project Exporter (`project_exporter`)**
*   **Purpose**: Dump project files into a single text file for LLM review.
*   **Command**: `python .ai/skills/project_exporter.py`
*   **Output**: `tool/output/project_for_llm_YYYYMMDD_HHMMSS.txt`

#### **Project Zip Exporter (`project_zip_exporter`)**
*   **Purpose**: Package project files into a zip with file manifest.
*   **Command**: `python .ai/skills/project_zip_exporter.py [options]`
*   **Output**: zip and manifest under `tool/output/` (see script help)

### Running Scripts
To avoid `ModuleNotFoundError`, always run scripts as modules from the project root:

```bash
# Full Pipeline
python start.py

# Single Strategy
python -m scripts.run_ma5_ma20_trend_strategy

# Open Monitor
python -m scripts.run_open_monitor

# Data Collection
python -c "from ashare.app import AshareApp; AshareApp().run()"
```

---

## 5. Data & Database

### Database Schema
Managed by `ashare.core.schema_manager`. Key standards:
- **Encoding**: MUST use `utf8mb4` charset to avoid Chinese character corruption.
- **Key Tables**:
- **`history_daily_kline`**: Daily OHLCV data.
- **`strategy_signal_events`**: Generated strategy signals.
- **`strategy_open_monitor_eval`**: Real-time monitoring results.
- **`strategy_open_monitor_env`**: Market environment snapshots.

### Local Data Handling
- Large datasets and output files in `tool/output/` are **ignored by git**.
- **Do NOT** upload these files to remote repositories.
- Use the **Raw Data Reader** skill to inspect them if needed.

---

## 6. Troubleshooting

- **`ModuleNotFoundError: No module named 'ashare'`**:
    - Fix: Run scripts using `python -m scripts.xxx` from the root directory.
- **`IntegrityError` (Duplicate Entry)**:
    - Context: Often happens when re-running strategies for the same date. This is expected behavior for idempotent runs.
