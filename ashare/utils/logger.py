"""项目统一日志配置。"""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Union

from ashare.config import get_section

_DEFAULT_FMT = "%(asctime)s [%(levelname)s] %(message)s"
_DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"
_DEFAULT_CONSOLE_FMT = "[%(levelname)s] %(message)s"


def _parse_level(value: str) -> int:
    value = value.strip().upper()
    if value.isdigit():
        return int(value)
    return getattr(logging, value, logging.INFO)


def _get_level_from_config() -> Optional[int]:
    section = get_section("logging")
    level_value = section.get("level")
    if level_value is None:
        return None
    if isinstance(level_value, int):
        return level_value
    if isinstance(level_value, str):
        return _parse_level(level_value)
    return _parse_level(str(level_value))


def _get_level_from_config_key(key: str) -> Optional[int]:
    section = get_section("logging")
    value = section.get(key)
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return _parse_level(value)
    return _parse_level(str(value))


def setup_logger(
    log_dir: Optional[Union[str, Path]] = None,
    level: int = logging.INFO,
    *,
    console_level: Optional[int] = None,
    file_level: Optional[int] = None,
    max_bytes: int = 20 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """配置并返回项目 logger（幂等，可重复调用）。

    - 默认写入 {log_dir}/ashare.log（log_dir 为空则写到项目根目录）。
    - 控制台 + 文件同时输出；文件使用滚动切分，避免日志无限增大。
    - 若设置了环境变量 ASHARE_LOG_LEVEL / ASHARE_LOG_DIR，或 config.yaml.logging.level，会覆盖默认值。
    """

    env_dir = os.getenv("ASHARE_LOG_DIR")
    if env_dir:
        log_dir = env_dir

    config_level = _get_level_from_config()
    if config_level is not None:
        level = config_level

    env_level = os.getenv("ASHARE_LOG_LEVEL")
    if env_level:
        level = _parse_level(env_level)

    if log_dir is None:
        log_dir_path = Path(__file__).resolve().parents[2]
    else:
        log_dir_path = Path(log_dir)

    log_dir_path.mkdir(parents=True, exist_ok=True)
    log_file = log_dir_path / "ashare.log"

    logger = logging.getLogger("ashare")

    if getattr(logger, "_ashare_configured", False):
        return logger

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    fmt = logging.Formatter(_DEFAULT_FMT, datefmt=_DEFAULT_DATEFMT)

    if console_level is None:
        console_level = _get_level_from_config_key("console_level")
    if file_level is None:
        file_level = _get_level_from_config_key("file_level")

    if console_level is None:
        console_level = level
    if file_level is None:
        file_level = level

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(logging.Formatter(_DEFAULT_CONSOLE_FMT))

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logging.getLogger().setLevel(logging.WARNING)
    for noisy in ("urllib3", "sqlalchemy.engine"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logger._ashare_configured = True  # type: ignore[attr-defined]
    logger.debug("日志已初始化：log_file=%s level=%s", log_file, level)
    return logger
