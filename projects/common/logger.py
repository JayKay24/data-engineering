import logging
import os
import sys


def get_logger(name: str = "DataEngineering") -> logging.Logger:
    """Configures (if not already configured) and returns a standard structured logger.

    Format: `YYYY-MM-DD HH:MM:SS [LEVEL] [logger_name] message`
    Level can be overridden via `LOG_LEVEL` environment variable (defaults to INFO).
    """
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Configure root/module handler with unified formatting if not already initialized
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stdout,
        )

    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    return logger
