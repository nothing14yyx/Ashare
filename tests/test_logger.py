import logging

import pytest

from ashare.utils import logger as logger_mod


@pytest.fixture(autouse=True)
def _reset_logger():
    logger = logging.getLogger("ashare")
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    if hasattr(logger, "_ashare_configured"):
        delattr(logger, "_ashare_configured")
    logger.setLevel(logging.NOTSET)
    logger.propagate = True
    yield
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    if hasattr(logger, "_ashare_configured"):
        delattr(logger, "_ashare_configured")


def test_parse_level():
    assert logger_mod._parse_level("10") == 10
    assert logger_mod._parse_level("info") == logging.INFO
    assert logger_mod._parse_level("notalevel") == logging.INFO


def test_setup_logger_writes_to_dir(monkeypatch, tmp_path):
    monkeypatch.delenv("ASHARE_LOG_DIR", raising=False)
    monkeypatch.delenv("ASHARE_LOG_LEVEL", raising=False)
    monkeypatch.setattr(logger_mod, "get_section", lambda name: {})

    log = logger_mod.setup_logger(log_dir=tmp_path)
    log.info("hello")
    assert (tmp_path / "ashare.log").exists()
