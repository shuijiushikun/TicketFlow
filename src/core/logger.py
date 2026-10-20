import json
import logging
import sys
from datetime import UTC, datetime

from src.core.config import settings
from src.core.request_context import request_id_context


class JsonFormatter(logging.Formatter):
    """将日志记录格式化为 JSON。

    包含时间、级别、模块、行号和异常信息，便于集中收集和分析日志。
    """

    def format(self, record: logging.LogRecord) -> str:
        """将一条日志记录转换为 JSON 字符串。"""
        log_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "module": record.module,
            "line": record.lineno,
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "extra_data"):
            log_entry.update(record.extra_data)

        log_entry["request_id"] = request_id_context.get()

        return json.dumps(log_entry)


class ConsoleFormatter(logging.Formatter):
    """为本地终端日志添加 ANSI 颜色，方便区分提示、警告和错误。"""

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format_str = "%(levelname)s:     [%(module)s:%(lineno)d] %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format_str + reset,
        logging.INFO: grey + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset,
    }

    def format(self, record: logging.LogRecord) -> str:
        """按日志级别选择颜色并生成终端输出文本。"""
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)

        return formatter.format(record)


def get_logger(name: str) -> logging.Logger:
    """获取或创建已配置的日志对象。

    name 通常使用调用模块的 __name__。已有处理器时直接复用，避免重复输出。
    日志级别取自配置；生产环境使用 JSON，本地开发使用彩色终端格式。
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    try:
        log_level = settings.LOG_LEVEL.upper()
        logger.setLevel(getattr(logging, log_level))
    except AttributeError:
        logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)

    if settings.ENVIRONMENT == "prod":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(ConsoleFormatter())

    logger.addHandler(handler)

    logger.propagate = False

    return logger


logger = get_logger("ticketcore")
