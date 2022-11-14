# type: ignore[attr-defined]
import os

from logger_config import config_logger

import uvicorn


config_logger()


def main() -> None:

    from executor_service.app import executor_app
    app = executor_app
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
