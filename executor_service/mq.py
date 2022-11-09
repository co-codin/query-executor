import logging
import asyncio
import pika
from contextlib import asynccontextmanager
from pika.adapters.asyncio_connection import AsyncioConnection

from settings import settings


LOG = logging.getLogger(__name__)
PIKA_CONNECTION = None


class PikaChannel:
    def __init__(self, channel):
        self._channel = channel

    async def basic_publish(self, exchange: str, routing_key: str, body: str):
        self._channel.basic_publish(exchange, routing_key, body)


async def create_connection():
    global PIKA_CONNECTION
    if PIKA_CONNECTION:
        return PIKA_CONNECTION

    loop = asyncio.get_running_loop()

    fut = loop.create_future()
    AsyncioConnection(
        pika.URLParameters(settings.mq_connection_string),
        on_open_callback=lambda c: fut.set_result(c),
        on_open_error_callback=lambda c, exc: fut.set_exception(exc),
        on_close_callback=lambda c, exc: fut.set_exception(exc)
    )
    conn = await fut
    PIKA_CONNECTION = conn
    return conn


@asynccontextmanager
async def create_channel() -> PikaChannel:
    loop = asyncio.get_running_loop()
    conn = await create_connection()

    fut = loop.create_future()
    conn.channel(on_open_callback=lambda ch: fut.set_result(ch))
    channel = await fut

    try:
        yield PikaChannel(channel)
    finally:
        channel.close()
