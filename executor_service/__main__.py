# type: ignore[attr-defined]
import os

from dotenv import load_dotenv

from logger_config import logger

import uvicorn

load_dotenv("../.env")


def main() -> None:
    from executor_service.app import executor_app
    app = executor_app
    port = int(os.environ.get("PORT", 8000))
    logger.info("Starting uvicorn!")
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
