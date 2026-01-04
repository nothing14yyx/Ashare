# AI Agent Custom Skills

本文档记录了为提升本项目的 AI 协作效率而开发的自定义技能（脚本工具）。所有工具脚本均存放于 `.ai/skills/` 目录下。

## 1. 核心基础技能

### 1.1 Raw Data Reader (绕过 .gitignore 读取)
*   **路径**: `.ai/skills/raw_reader.py`
*   **用途**: 读取被 git 忽略的本地数据文件（如 `tool/output/` 下的 JSON/Log）。
*   **命令**: `python .ai/skills/raw_reader.py [read|list] <path>`
*   **说明**: 技能脚本按文档命令直接执行，不需要 `python -m`。

### 1.2 Database Query (直接查询数据库)
*   **路径**: `.ai/skills/db_query.py`
*   **用途**: 直接执行 SQL 查询 MySQL 数据库。
*   **命令**: `python .ai/skills/db_query.py "<SQL_QUERY>"`
*   **注意**: 查询如 `strategy_board_rotation` 等时间序列数据表时，建议使用 `ORDER BY date DESC` 以获取最新的数据记录，例如：`SELECT * FROM strategy_board_rotation ORDER BY date DESC LIMIT 20;`

---

## 2. 市场与数据分析技能

### 2.1 Market Environment Analyzer (市场环境诊断)
*   **路径**: `.ai/skills/market_env_analyzer.py`
*   **用途**: 调用 `WeeklyEnvironmentBuilder` 生成当前市场的多维评估报告（风险等级、仓位建议、周线情景）。
*   **命令**: `python .ai/skills/market_env_analyzer.py [YYYY-MM-DD]`

### 2.2 Sector Rotation Analyzer (板块轮动分析)
*   **路径**: `.ai/skills/sector_rotation_analyzer.py`
*   **用途**: 基于趋势+动量模型分析板块轮动象限（领涨、转强、转弱、滞后）。
*   **命令**: `python .ai/skills/sector_rotation_analyzer.py [YYYY-MM-DD]`

---

## 3. 项目维护技能

### 3.1 DB Snapshot Exporter (数据库快照导出)
*   **路径**: `.ai/skills/db_snapshot_exporter.py`
*   **用途**: 导出数据库所有表的结构统计和样例数据（生成 Markdown/JSON），帮助理解数据全貌。
*   **命令**: `python .ai/skills/db_snapshot_exporter.py`
*   **输出**: `tool/output/db_snapshot_*.md` 和 `tool/output/db_snapshot_*.json`

### 3.2 Project Exporter (项目源码导出)
*   **路径**: `.ai/skills/project_exporter.py`
*   **用途**: 将项目核心源码合并为一个文本文件，便于进行代码审查或上下文补充。
*   **命令**: `python .ai/skills/project_exporter.py`
*   **输出**: `tool/output/project_for_llm_*.txt`

### 3.3 Project Zip Exporter (项目打包)
*   **路径**: `.ai/skills/project_zip_exporter.py`
*   **用途**: 将项目打包为 zip（自动忽略 venv、git 等目录）。
*   **命令**: `python .ai/skills/project_zip_exporter.py [options]`

### 3.4 Environment & Config Tester (环境自检)
*   **路径**: `.ai/skills/env_tester.py`
*   **用途**: 诊断项目环境，检查配置文件加载、数据库连接状态、重构视图可用性以及第三方数据源（AkShare/Baostock）的依赖情况。
*   **命令**: `python .ai/skills/env_tester.py`

---

## 4. 网络与数据源测试技能

### 4.1 AkShare Network Tester
*   **路径**: `.ai/skills/network_tester_akshare.py`
*   **用途**: 自检 AkShare 接口连通性（含代理/非代理对比）。
*   **命令**: `python .ai/skills/network_tester_akshare.py`

### 4.2 Baostock Network Tester
*   **路径**: `.ai/skills/network_tester_baostock.py`
*   **用途**: 自检 Baostock 接口连通性。
*   **命令**: `python .ai/skills/network_tester_baostock.py`
