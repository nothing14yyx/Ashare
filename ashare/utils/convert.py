"""通用数值转换工具。"""

from __future__ import annotations

import math
from typing import Any


def to_float(value: Any) -> float | None:  # noqa: ANN401
    """尽可能安全地将输入转换为 float。"""

    try:
        if value is None:
            return None
        if isinstance(value, str):
            v = value.strip()
            if v in {"", "-", "--", "None", "nan"}:
                return None
            return float(v.replace(",", ""))
        if isinstance(value, (int, float)):
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                return None
            return float(value)
        return float(value)
    except Exception:
        return None
