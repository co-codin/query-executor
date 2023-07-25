import asyncio
import pika

from contextlib import asynccontextmanager

from pika.channel import Channel
from pika.adapters.asyncio_connection import AsyncioConnection

from executor_service.settings import settings


class PikaChannel:
    conn: AsyncioConnection | None = None

    def __init__(self, channel: Channel):
        self._channel = channel
        self._close_callbacks = []

    async def basic_publish(self, exchange: str, routing_key: str, body: bytes):
        self._channel.basic_publish(exchange, routing_key, body)


async def create_connection():
    if PikaChannel.conn:
        return PikaChannel.conn

    loop = asyncio.get_running_loop()

    fut = loop.create_future()
    AsyncioConnection(
        pika.URLParameters(settings.mq_connection_string),
        on_open_callback=lambda c: fut.set_result(c),
        on_open_error_callback=lambda c, exc: fut.set_exception(exc),
        on_close_callback=lambda c, exc: fut.set_exception(exc)
    )
    conn = await fut
    PikaChannel.conn = conn
    return PikaChannel.conn


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
        PikaChannel.conn = None
        if channel.is_open:
            channel.close()
