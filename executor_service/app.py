# type: ignore[attr-defined]
import asyncio
import os
import logging
from typing import Callable

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from executor_service.endpoints import queries, keys, publications
from executor_service.services.publish_request_lifespan import publish_request
from executor_service.errors import APIError
from executor_service.auth import load_jwks
from executor_service.mq import create_channel
from executor_service.settings import settings

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    logger.info("Starting query executor!")
    app = FastAPI(
        title="Query executor app",
        description="API for query executor app",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app


executor_app = create_app()

current_dir = os.path.dirname(os.path.abspath(__file__))
executor_app.include_router(queries.router, prefix='/v1')
executor_app.include_router(publications.router, prefix='/v1')
executor_app.include_router(keys.router, prefix='/v1')


@executor_app.on_event('startup')
async def on_startup():
    await load_jwks()

    async with create_channel() as channel:
        await channel.exchange_declare(settings.publish_exchange, 'direct')

        await channel.queue_declare(settings.publish_request_queue)
        await channel.queue_bind(settings.publish_request_queue, settings.publish_exchange, 'task')

        await channel.queue_declare(settings.publish_result_queue)
        await channel.queue_bind(settings.publish_result_queue, settings.publish_exchange, 'result')

        asyncio.create_task(consume(settings.publish_request_queue, publish_request))


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


async def consume(query, func: Callable):
    while True:
        try:
            logger.info(f'Starting {query} worker')
            async with create_channel() as channel:
                async for delivery_tag, body in channel.consume(query):
                    try:
                        await func(body, channel)
                        await channel.basic_ack(delivery_tag)
                    except Exception as e:
                        logger.exception(f'Failed to process message {body}: {e}')
                        await channel.basic_reject(delivery_tag, requeue=False)
        except Exception as e:
            logger.exception(f'Worker {query} failed: {e}')

        await asyncio.sleep(0.5)
