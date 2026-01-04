# Database Design Standards (MySQL)

## Naming

- Use `snake_case` for schemas, tables, and columns.
- Prefer singular table names (e.g., `user_account`).
- Use `_id` suffix for foreign keys.

## Primary keys

- Default to `id` as `BIGINT` auto-increment.
- Use UUIDs only when there is a clear need (distributed writes, offline generation).

## Column types

- Use `DECIMAL` for money; avoid `FLOAT` for currency.
- Use `DATETIME` in UTC for timestamps.
- Use `TINYINT(1)` for booleans.

## Constraints

- Prefer `NOT NULL` with explicit defaults for required fields.
- Add `UNIQUE` constraints for natural keys.
- Enforce foreign keys unless there is a documented performance reason not to.

## Indexing

- Index foreign keys and common filter/join columns.
- Order composite indexes by selectivity.
- Avoid redundant or overlapping indexes.

## Migrations

- Apply all schema changes through migrations.
- Prefer backward-compatible changes first (add columns before dropping).
- Provide rollback steps or reversible migrations.

## Query rules

- Avoid `SELECT *`; list explicit columns.
- Use parameterized queries.
- Use `LIMIT` for pagination and avoid unbounded scans.

## Audit fields

- Include `created_at` and `updated_at`.
- Add `deleted_at` for soft deletes when needed.
