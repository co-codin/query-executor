import os
import logging
import ecs_logging

from logging.handlers import TimedRotatingFileHandler

from executor_service.settings import settings


def config_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if settings.debug else logging.INFO)
    os.makedirs(settings.log_dir, exist_ok=True)

    logging.getLogger('pika').setLevel(logging.WARNING)

    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)

    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(settings.log_dir, settings.log_name),
        when=settings.time_period_unit,
        backupCount=settings.backup_count * 2,
        encoding=settings.encoding
    )
    file_handler.setFormatter(ecs_logging.StdlibFormatter())
    logger.addHandler(file_handler)
