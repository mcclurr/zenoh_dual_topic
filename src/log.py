import logging
from datetime import datetime
from pathlib import Path


def init_logging(prefix: str) -> logging.Logger:
    """
    Initialize structured logging.

    Creates:
        out/YYYYMMDD_HHMMSS-<prefix>.log

    Returns:
        configured logger
    """
    log_dir = Path("out")
    log_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = log_dir / f"{ts}-{prefix}.log"

    logger = logging.getLogger(prefix)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # prevent duplicate root logging

    # Prevent duplicate handlers if reinitialized
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(filename)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logging initialized → {filename}")

    return logger