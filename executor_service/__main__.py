# type: ignore[attr-defined]
from executor_service.logger_config import config_logger
from executor_service.settings import settings

import uvicorn


config_logger()


if __name__ == "__main__":
    uvicorn.run("executor_service.app:executor_app", host="0.0.0.0", port=settings.port, reload=settings.reload)

