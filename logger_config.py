import json
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from typing import Tuple

from settings import settings


class LogLevelFilter(logging.Filter):
    def __init__(self, level):
        super().__init__()
        self.__level = level

    def filter(self, logRecord):  # noqa
        return logRecord.levelno <= self.__level


class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings after parsing the LogRecord.

    @param dict fmt_dict: Key: logging format attribute pairs.
    Defaults to {"message": "message"}.
    @param str time_format: time.strftime() format string.
    Default: "%Y-%m-%d %H:%M:%S"
    """

    def __init__(self, fmt_dict: dict = None,
                 time_format: str = "%Y-%m-%dT%H:%M:%S",
                 msec_format: str = "%s.%03dZ"):
        super().__init__()
        self.fmt_dict = fmt_dict if fmt_dict is not None \
            else {"message": "message"}
        self.default_time_format = time_format
        self.default_msec_format = msec_format
        self.date_fmt = None

    def usesTime(self) -> bool:
        """
        Overwritten to look for the attribute in the format dict values
        instead of the fmt string.
        """
        return "asctime" in self.fmt_dict.values()

    def formatMessage(self, record) -> dict:
        """
        Overwritten to return a dictionary of the relevant LogRecord
        attributes instead of a string.
        KeyError is raised if an unknown attribute is provided in the fmt_dict.
        """
        return {fmt_key: record.__dict__[fmt_val] for fmt_key, fmt_val in
                self.fmt_dict.items()}

    def format(self, record: logging.LogRecord) -> str:
        """
        Mostly the same as the parent's class method, the difference being
        that a dict is manipulated and dumped as JSON instead of a string.
        """
        record.message = record.getMessage()

        if self.usesTime():
            record.asctime = self.formatTime(record, self.date_fmt)

        message_dict = self.formatMessage(record)

        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)

        if record.exc_text:
            message_dict["exc_info"] = record.exc_text

        if record.stack_info:
            message_dict["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(message_dict, default=str)


def config_logger():
    log_folder = _config_log_dir()
    logger = _set_log_level()
    json_file_main_handler, json_file_error_handler = \
        _config_handlers(log_folder)
    json_file_main_formatter, json_file_error_formatter = _config_formatters()

    json_file_main_handler.setFormatter(json_file_main_formatter)
    json_file_error_handler.setFormatter(json_file_error_formatter)

    json_file_main_handler.addFilter(LogLevelFilter(logging.WARNING))

    logger.addHandler(json_file_main_handler)
    logger.addHandler(json_file_error_handler)


def _config_log_dir() -> str:
    configs_dir_path = os.path.abspath(os.path.dirname(__file__))
    base_dir = os.path.join(configs_dir_path, '../../')
    log_folder = f"{base_dir}/{settings.log_dir}"
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    return log_folder


def _set_log_level() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger


def _config_handlers(log_folder: str) -> Tuple[
    TimedRotatingFileHandler, TimedRotatingFileHandler
]:
    json_file_main_handler = TimedRotatingFileHandler(
        filename=f"{log_folder}/main.json",
        when=settings.time_period_unit,
        backupCount=settings.backup_count * 2,
        encoding=settings.encoding
    )
    json_file_error_handler = TimedRotatingFileHandler(
        filename=f"{log_folder}/errors.json",
        when=settings.time_period_unit,
        backupCount=settings.backup_count * 2,
        encoding=settings.encoding
    )

    json_file_main_handler.setLevel(logging.INFO)
    json_file_error_handler.setLevel(logging.ERROR)
    return json_file_main_handler, json_file_error_handler


def _config_formatters() -> Tuple[JsonFormatter, JsonFormatter]:
    json_file_main_formatter = JsonFormatter(
        {
            "timestamp": "asctime",
            "loggerName": "name",
            "level": "levelname",
            "message": "message",
        },
        time_format=settings.date_time_format
    )
    json_file_error_formatter = JsonFormatter(
        {
            "timestamp": "asctime",
            "loggerName": "name",
            "level": "levelname",
            "module": "module",
            "funcName": "funcName",
            "lineNumber": "lineno",
            "message": "message",
        },
        time_format=settings.date_time_format
    )
    return json_file_main_formatter, json_file_error_formatter


config_logger()
logger = logging.getLogger(__name__)
logger.info("Test info log message")
logger.warning("Test warning log message")
logger.error("Test error log message")
try:
    raise Exception("Test exception log message")
except Exception as exc:
    logger.exception(exc)
