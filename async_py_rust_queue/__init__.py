import asyncio
import os
import sys
from typing import Tuple
import collections
import datetime
import psutil
import objgraph

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


async def to_test_inner(qr: QueueReceiver, cb):
    fd = qr.fd
    total = 0
    while True:
        len, did_close = await wait_on_eventfd(fd)
        total += len

        # print(f"to read: {len=}, {did_close=}, {total=}")

        vals = qr.get_items(len)

        for val in vals:
            await cb(val)

        if did_close:
            break

        # print(f"{len=} {vals=}")
    print("python side stopped")


def print_stats(process, dispatches, uptime):
    memory_usage = process.memory_full_info().uss
    memory_usage = memory_usage / 1024 ** 2
    cpu_usage = process.cpu_percent() / psutil.cpu_count()
    print(f"CPU usage: {cpu_usage:.2f}%")
    print(f"Memory Usage: {memory_usage:.2f} MiB")
    for key, value in dispatches.items():
        print(f"Worker ID {key} Dispatches: {value}")

    total = sum(dispatches.values())
    delta = datetime.datetime.utcnow() - uptime
    seconds = delta.total_seconds() or 1.0
    events_per_second = total / seconds
    print(f"Total: {total} ({events_per_second:.2f}/s)")
    print(f"Common Types:")
    objgraph.show_most_common_types()


async def do_test():
    seconds = 10
    print(f"Testing for {seconds=}")
    process = psutil.Process()
    start = datetime.datetime.utcnow()

    rt = create_rt()
    qr: QueueReceiver = launch_appenders(1, rt)
    dispatches = collections.Counter()
    print_stats(process, dispatches, start)

    async def callback(v):
        dispatches[v.worker_id] += 1

    t = asyncio.create_task(to_test_inner(qr, callback))
    await asyncio.sleep(seconds)
    qr.stop()
    await t
    print("Done testing")
    print_stats(process, dispatches, start)
