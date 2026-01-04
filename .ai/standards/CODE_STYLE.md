# Code Style & Development Standards

## 1. Python Guidelines
- **Version**: Python 3.10+
- **Formatting**: PEP 8.
    - Indentation: 4 spaces.
    - Max Line Length: 100-120 chars (be reasonable).
- **Type Hinting**: **REQUIRED**.
    - Use `typing.List`, `typing.Dict`, `typing.Optional` or Python 3.10+ `|` syntax.
    - Example: `def fetch_data(code: str) -> pd.DataFrame | None:`

## 2. Project-Specific Rules

### A. Imports & Paths
- **Absolute Imports**: Always use absolute imports.
    - ✅ `from ashare.core.db import MySQLWriter`
    - ❌ `from ..core.db import MySQLWriter`
- **Script Execution**:
    - Scripts in `scripts/` MUST add project root to `sys.path` if they are intended to be run directly.
    ```python
    import sys, os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ```

### B. Configuration
- **No Hardcoding**: Database credentials, API keys, and strategy parameters must come from `config.yaml`.
- **Params Class**: Use `dataclass` for strategy parameters (e.g., `MA5MA20Params`).

### C. Error Handling
- **Log, Don't Print**: Use `logging.getLogger("ashare")`.
- **Fail Gracefully**: In loops (e.g., processing 100 stocks), one failure should not crash the entire process. Log the error and `continue`.

## 3. Testing
- **Framework**: `pytest`.
- **Scope**:
    - Unit tests for utility functions.
    - Integration tests for Database read/write.
    - **Note**: Network tests (Baostock/AkShare) should be marked or separated to avoid slowing down CI.
