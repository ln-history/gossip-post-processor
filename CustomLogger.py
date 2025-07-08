import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any


class CustomLogger:
    TRACE_LEVEL_NUM = 5

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    @staticmethod
    def create(log_dir: str = "logs", log_file: str = "gossip_post_processor.log") -> "CustomLogger":
        # Register TRACE level
        logging.addLevelName(CustomLogger.TRACE_LEVEL_NUM, "TRACE")

        def trace(self: logging.Logger, message: str, *args: Any, **kwargs: Any) -> None:
            if self.isEnabledFor(CustomLogger.TRACE_LEVEL_NUM):
                self._log(CustomLogger.TRACE_LEVEL_NUM, message, args, **kwargs)

        logging.Logger.trace = trace  # type: ignore[attr-defined]

        os.makedirs(log_dir, exist_ok=True)
        logger = logging.getLogger("gossip_post_processor")
        logger.setLevel(logging.DEBUG)  # Enable TRACE and DEBUG levels

        log_path = os.path.join(log_dir, log_file)
        file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        return CustomLogger(logger)

    def trace(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.trace(message, *args, **kwargs)  # type: ignore[attr-defined]

    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.debug(message, *args, **kwargs)

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.info(message, *args, **kwargs)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.warning(message, *args, **kwargs)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.error(message, *args, **kwargs)

    def critical(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.critical(message, *args, **kwargs)

    def exception(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.logger.exception(message, *args, **kwargs)
