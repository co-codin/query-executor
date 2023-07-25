# type: ignore[attr-defined]
import os
import logging

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from executor_service.endpoints import queries, keys
from executor_service.errors import APIError
from executor_service.auth import load_jwks

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    logger.info("Starting query executor!")
    app = FastAPI(
        title="Query executor app",
        description="API for query executor app",
    )

    return app


executor_app = create_app()

current_dir = os.path.dirname(os.path.abspath(__file__))
executor_app.include_router(queries.router, prefix="/v1")
executor_app.include_router(keys.router, prefix="/v1")


@executor_app.on_event('startup')
async def on_startup():
    await load_jwks()


@executor_app.middleware("http")
async def request_log(request: Request, call_next):
    """
    Global exception handler for catching non API errors.
    ALso catch, sort and write uvicorn output and critical errors to log
    :param request: Request
    :param call_next: call_next
    :return: JSONResponse
    """
    try:
        response: Response = await call_next(request)
        if response.status_code < 400:
            logger.info(f"{request.method} {request.url} Status code: {response.status_code}")
        else:
            logger.warning(f"{request.method} {request.url} Status code: {response.status_code}")
        return response
    except Exception as exc:  # noqa
        logger.exception(str(exc))
        return JSONResponse(
            status_code=500,
            content={"message": "Something went wrong!"},
        )


@executor_app.get("/health")
def health() -> JSONResponse:
    """
    Health check endpoint
    :return: JSONResponse
    """
    return JSONResponse(
        status_code=200,
        content={"message": "health check successful"},
    )

@executor_app.exception_handler(APIError)
def api_exception_handler(_request: Request, exc: APIError) -> JSONResponse:
    """
    Exception handler for catching API errors
    :param _request: Request
    :param exc: APIError
    :return: JSONResponse
    """
    logger.warning(exc.message)
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": exc.message},
    )
