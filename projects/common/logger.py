"""Centralized structured logging factory for monorepo pipelines and jobs."""

import logging
import os
import sys


def get_logger(name: str = "DataEngineering") -> logging.Logger:
    """Configure and return a structured stdout logger.

    Configures root logging with the format
    ``YYYY-MM-DD HH:MM:SS [LEVEL] [logger_name] message`` if no root handlers
    exist yet. The log level can be overridden via the ``LOG_LEVEL``
    environment variable (defaults to ``INFO``).

    Args:
        name: Logger name displayed in log records.

    Returns:
        Configured ``logging.Logger`` instance.
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
