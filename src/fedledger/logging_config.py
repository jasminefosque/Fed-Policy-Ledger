"""Structured logging for Fed Policy Ledger.

This module provides a structured JSON logging system with context-aware
formatting for tracking document processing pipelines.
"""

import logging
import json
import sys
import traceback
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path


class StructuredFormatter(logging.Formatter):
    """Custom formatter that outputs structured JSON logs.
    
    Each log record includes:
    - timestamp (ISO 8601)
    - level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - message
    - logger name
    - Optional context fields (doc_id, extractor, etc.)
    - Exception details if present
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON.
        
        Args:
            record: Log record to format.
        
        Returns:
            JSON string representation of the log record.
        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add context fields if present
        if hasattr(record, "doc_id"):
            log_data["doc_id"] = record.doc_id
        if hasattr(record, "extractor"):
            log_data["extractor"] = record.extractor
        if hasattr(record, "source_url"):
            log_data["source_url"] = record.source_url
        if hasattr(record, "doc_type"):
            log_data["doc_type"] = record.doc_type
        
        # Add exception information if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info),
            }
        
        # Add any extra fields passed to the logger
        for key, value in record.__dict__.items():
            if key not in [
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "message", "pathname", "process", "processName", "relativeCreated",
                "thread", "threadName", "exc_info", "exc_text", "stack_info",
                "doc_id", "extractor", "source_url", "doc_type"
            ]:
                log_data[key] = value
        
        return json.dumps(log_data)


class ColoredFormatter(logging.Formatter):
    """Colored formatter for human-readable console output."""
    
    COLORS = {
        "DEBUG": "\033[36m",      # Cyan
        "INFO": "\033[32m",       # Green
        "WARNING": "\033[33m",    # Yellow
        "ERROR": "\033[31m",      # Red
        "CRITICAL": "\033[1;31m", # Bold Red
    }
    RESET = "\033[0m"
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors.
        
        Args:
            record: Log record to format.
        
        Returns:
            Colored string representation of the log record.
        """
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        
        # Add context to message if present
        context_parts = []
        if hasattr(record, "doc_id"):
            context_parts.append(f"doc_id={record.doc_id}")
        if hasattr(record, "extractor"):
            context_parts.append(f"extractor={record.extractor}")
        
        if context_parts:
            record.msg = f"{record.msg} [{', '.join(context_parts)}]"
        
        return super().format(record)


def setup_logging(
    level: str = "INFO",
    json_output: bool = False,
    log_file: Optional[Path] = None
) -> logging.Logger:
    """Set up logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        json_output: If True, output structured JSON logs to console.
        log_file: Optional file path for logging output.
    
    Returns:
        Configured root logger.
    """
    logger = logging.getLogger("fedledger")
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    
    if json_output:
        console_handler.setFormatter(StructuredFormatter())
    else:
        colored_fmt = ColoredFormatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(colored_fmt)
    
    logger.addHandler(console_handler)
    
    # File handler (always JSON format for structured logs)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(StructuredFormatter())
        logger.addHandler(file_handler)
    
    return logger


class LoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds context fields to log records.
    
    Usage:
        logger = get_logger(__name__)
        doc_logger = LoggerAdapter(logger, {"doc_id": "abc123"})
        doc_logger.info("Processing document")
    """
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Process log message and add context fields.
        
        Args:
            msg: Log message.
            kwargs: Keyword arguments for logging call.
        
        Returns:
            Tuple of (message, kwargs) with context added.
        """
        # Add context fields as extra attributes
        extra = kwargs.get("extra", {})
        extra.update(self.extra)
        kwargs["extra"] = extra
        return msg, kwargs


def get_logger(name: str, **context) -> logging.Logger:
    """Get a logger with optional context.
    
    Args:
        name: Logger name (usually __name__).
        **context: Context fields to add to all log records.
    
    Returns:
        Logger or LoggerAdapter with context.
    """
    logger = logging.getLogger(f"fedledger.{name}")
    
    if context:
        return LoggerAdapter(logger, context)
    
    return logger
