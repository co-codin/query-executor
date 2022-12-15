import io

import msgpack
import typing
from datetime import datetime, timezone
from contextlib import contextmanager


__all__ = [
    'msgpack_reader',
    'msgpack_writer'
]


def handle_datetime(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc)
    raise TypeError(f'Value {repr(value)} is not serializable')


class PickleIO:
    LEN_SIZE = 8
    def __init__(self, fd: typing.BinaryIO):
        self._fd = fd


class MsgpackWriter(PickleIO):
    def writerow(self, row: typing.Any):
        data = msgpack.packb(row, default=handle_datetime, datetime=True)
        self._fd.write(len(data).to_bytes(self.LEN_SIZE, byteorder='big'))
        self._fd.write(data)

    def writerows(self, rows: typing.List[typing.Any]):
        for row in rows:
            self.writerow(row)


class MsgpackReader(PickleIO):
    class Eof(Exception):
        pass

    def __iter__(self):
        try:
            while True:
                row = self.readrow()
                yield row
        except self.Eof:
            pass

    def readrow(self) -> typing.Any:
        length = self._fd.read(self.LEN_SIZE)
        if not length:
            raise self.Eof()
        length = int.from_bytes(length, byteorder='big')
        data = self._fd.read(length)
        return msgpack.unpackb(data, timestamp=3)


@contextmanager
def msgpack_writer(path: str) -> MsgpackWriter:
    with open(path, 'wb') as fd:
        yield MsgpackWriter(fd)


@contextmanager
def msgpack_reader(path: str) -> MsgpackReader:
    with open(path, 'rb') as fd:
        yield MsgpackReader(fd)
