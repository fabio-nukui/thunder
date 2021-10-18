import json
import logging


class ExtraDataFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if data := getattr(record, "data", None):
            return f"{super().format(record)}; data={json.dumps(data)}"
        return super().format(record)
