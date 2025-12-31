# Code Standards (Python)

## Scope and boundaries

- Keep all domain logic inside the primary package (e.g., `src/<package>` or `<package>/`).
- Entry points (CLI, run scripts, schedulers) orchestrate only: parse args, load config, call package APIs.
- Do not import from ad-hoc directories like `tool/` or `scripts/` inside the core package.
- Avoid cross-project imports unless explicitly approved.

## Structure and size

- Keep modules cohesive and small; split when a file exceeds 400 lines.
- Limit individual functions to 60 lines and classes to 200 lines.
- Avoid generic "utils.py" dumping grounds; group helpers by domain.

## Naming and style

- Follow PEP 8: `snake_case` for functions/variables, `CamelCase` for classes, `UPPER_CASE` for constants.
- Prefer explicit, descriptive names over abbreviations.

## Configuration and secrets

- Never hardcode credentials or API keys.
- Read configuration from config files or environment variables.
- Keep config parsing in a dedicated module.

## Dependencies

- Prefer standard library when possible.
- Add third-party dependencies only when justified and used.
- Pin versions in requirements files.

## Data access

- Use a data access layer (repository/DAO) for database calls.
- Do not embed raw SQL in business/service modules unless explicitly approved.

## Errors and logging

- Catch specific exceptions; avoid bare `except`.
- Use the `logging` module; avoid `print` outside CLI tools.

## Tests

- New behavior should include unit tests, or a short rationale for why tests are not added.
