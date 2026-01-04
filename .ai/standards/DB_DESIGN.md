# Database Design Standards

> **Core Principle**: Performance & Clarity. Quantitative data grows fast; design for read-heavy workloads.

## 1. Naming Conventions
- **Tables**: `snake_case`. Prefix with domain:
    - `history_`: Raw market data (e.g., `history_daily_kline`).
    - `strategy_`: Derived strategy data (e.g., `strategy_signal_events`).
    - `meta_` / `dim_`: Metadata or dimension tables (e.g., `dim_stock_basic`).
- **Columns**: `snake_case`.
    - Date: `trade_date` (for business date), `created_at` (for system timestamp).
    - Money: `volume` (share count), `amount` (currency value).

## 2. Field Types
- **Encoding**: 
    - **Charset**: `utf8mb4` (Required for Chinese characters).
    - **Collation**: `utf8mb4_unicode_ci` or `utf8mb4_general_ci`.
- **Primary Keys**: 
    - Prefer composite keys for time-series data: `(trade_date, code)` is better than an auto-increment ID for K-lines.
    - Use `BIGINT` for surrogate keys (`run_pk`).
- **Decimals**:
    - Use `DOUBLE` for high-frequency calculation fields (Python `float` compatibility).
    - Use `DECIMAL(18,4)` for strict financial accounting (balance, PnL).
- **Strings**:
    - `VARCHAR(20)` for stock codes (`sh.600000`).
    - `VARCHAR(32)` for enums/tags (`strategy_code`).

## 3. Constraints & Indexes
- **Foreign Keys**: **FORBIDDEN**. Maintain integrity in the application layer (`ashare.core`) to avoid locking issues during bulk inserts.
- **Indexes**:
    - Essential for query performance.
    - Always index `trade_date` and `code` individually or compositely.
    - Use `UNIQUE` constraints to prevent data duplication (idempotency).

## 4. Operation Guidelines
- **Idempotency**: Insert logic must handle duplicates (e.g., `INSERT IGNORE` or delete-before-write).
- **Migrations**: Do not use migration tools (Alembic) for core data tables. Use `SchemaManager` (`ashare.core.schema_manager`) to ensure schema consistency at runtime.
