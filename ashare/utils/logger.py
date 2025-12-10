"""日志工具."""

import logging
from pathlib import Path


def setup_logger(output_dir: Path) -> logging.Logger:
    """配置基础日志记录器，输出到文件和控制台."""

    log_file = output_dir / "ashare.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger("ashare")
