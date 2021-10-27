from __future__ import annotations

import json
import logging
from typing import Callable


class ExtraDataFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if data := getattr(record, "data", None):
            return f"{super().format(record)}; data={json.dumps(data)}"
        return super().format(record)


class ReformatedLogger:
    def __init__(self, name: str | None, formater: Callable):
        self._logger = logging.getLogger(name)
        self.formater = formater

    def debug(self, msg, *args, **kwargs):
        self._logger.debug(self.formater(msg), *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._logger.info(self.formater(msg), *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._logger.warning(self.formater(msg), *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._logger.error(self.formater(msg), *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self._logger.critical(self.formater(msg), *args, **kwargs)
