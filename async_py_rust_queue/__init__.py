import asyncio
import os
import sys
from typing import Tuple

from .async_py_rust_queue import QueueReceiver, launch_appenders, create_rt

int64_min = -9223372036854775808

async def wait_on_eventfd(fd: int) -> Tuple[int, bool]:
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    loop.add_reader(fd, fut.set_result, None)
    await fut
    loop.remove_reader(fd)
    bs = os.read(fd, 8)
    n = int.from_bytes(bs, byteorder=sys.byteorder, signed=True)
    if n < 0:
        return (n - int64_min, True)
    return (n, False)

async def do_test():
    rt = create_rt()
    qr: QueueReceiver = launch_appenders(rt)
    fd = qr.fd
    total = 0
    while True:
        len, did_close = await wait_on_eventfd(fd)
        total += len
        print(f"to read: {len=}, {did_close=}, {total=}")
        vals = qr.get_items(len)
        if did_close:
            break

        print(f"{len=} {vals=}")

    expected = 8 * 1000
    if total != expected:
        print(f"didn't receive all events!! {total=} {expected=}")
