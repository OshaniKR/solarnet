"""
Structured JSON logging, shared by every component (simulators, Spark job, Airflow
DAG, API). Every log line is a single JSON object with at least:
    timestamp, level, component, message, event
so log lines can be grepped / shipped to any log aggregator uniformly, and so the
report's Observability section can point at one consistent format across the stack.
"""
import json
import logging
import sys


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, event: str, **fields):
    """Log one structured event, e.g. log_event(logger, 'batch_processed', count=42)."""
    logger.info(event, extra={"extra_fields": {"event": event, **fields}})
