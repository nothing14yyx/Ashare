---
name: project-standards
description: Project-wide coding and database design standards for Python + MySQL codebases. Use when adding or modifying Python modules, project structure, data access layers, SQL queries, or schema/migrations; and when reviewing for scope creep or standards compliance.
---

# Project Standards

## Overview

Keep Python + MySQL projects within clear boundaries: consistent structure, stable module ownership, and safe schema changes. Apply these rules for any code or database change.

## How to Apply

1. Identify scope: code, database, or both.
2. Load the right references:
   - Code rules: `references/code_standards.md`
   - DB rules: `references/db_design.md`
3. Enforce boundaries:
   - Keep orchestration in entry points; keep domain logic in modules.
   - Prevent cross-layer imports and direct DB usage in business logic unless explicitly approved.
4. Run the audit script when changes are non-trivial:
   - `python scripts/standards_audit.py --root .`
   - Tune thresholds via flags if the project already has larger files.

## When to Ask

- Unclear module ownership or layering.
- A change needs new dependencies, schema changes, or cross-package imports.
- The requested change conflicts with size limits or schema rules.

## Resources

### scripts/

`scripts/standards_audit.py`: Lightweight checks for size limits, star imports, and forbidden patterns/imports.

### references/

- `references/code_standards.md`: Python structure, naming, layering, config, logging, tests, and size limits.
- `references/db_design.md`: MySQL schema, constraints, indexing, migrations, and query rules.
