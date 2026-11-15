"""多连接编排：公共、业务、私有、策略委托四条连接 + 一个写线程。"""
import asyncio

from ..setting import WS_BATCH_SIZE, WS_FLUSH_INTERVAL, WS_QUEUE_SIZE
from .private import BusinessPrivateSubscribe, PrivateSubscribe
from .public import CandleSubscribe, PublicSubscribe
from .writer import StreamWriter

_ENDPOINT_TO_ATTR = ("public", "business", "private", "algo")


class SubscribeManager:
    """持有 Kernel 的全部 WebSocket 连接与共享 StreamWriter。"""

    def __init__(self, kernel):
        self.kernel = kernel
        self.writer = StreamWriter(
            kernel.dolphin,
            batch_size=WS_BATCH_SIZE,
            flush_interval=WS_FLUSH_INTERVAL,
            queue_size=WS_QUEUE_SIZE,
        )
        reconciler = getattr(kernel, "reconciler", None)
        self.public = PublicSubscribe(
            kernel, kernel.publicChannels, writer=self.writer, reconciler=reconciler,
        )
        self.business = CandleSubscribe(
            kernel, kernel.candleChannels, writer=self.writer, reconciler=reconciler,
        )
        self.private = None
        self.algo = None
        self._tasks = []

    def start(self):
        """在 Kernel 的事件循环线程上调度各连接的 run 协程。"""
        self._tasks.append(asyncio.run_coroutine_threadsafe(self.public.run(), self.kernel._loop))
        self._tasks.append(asyncio.run_coroutine_threadsafe(self.business.run(), self.kernel._loop))
        if self.kernel.private_enabled:
            self.private = PrivateSubscribe(
                self.kernel, self.kernel.privateChannels,
                writer=self.writer, reconciler=getattr(self.kernel, "reconciler", None),
            )
            self._tasks.append(asyncio.run_coroutine_threadsafe(self.private.run(), self.kernel._loop))
        if self.kernel.algo_enabled:
            self.algo = BusinessPrivateSubscribe(
                self.kernel, self.kernel.algoChannels,
                writer=self.writer, reconciler=getattr(self.kernel, "reconciler", None),
            )
            self._tasks.append(asyncio.run_coroutine_threadsafe(self.algo.run(), self.kernel._loop))

    def _connection(self, endpoint: str):
        connection = {
            "public": self.public,
            "business": self.business,
            "private": self.private,
            "business-private": self.algo,
        }.get(endpoint)
        if connection is None:
            raise RuntimeError(f"{endpoint} 连接未启用")
        return connection

    def subscribe(self, endpoint: str, args):
        """动态订阅（不重启进程）。"""
        return asyncio.run_coroutine_threadsafe(
            self._connection(endpoint).subscribe(args), self.kernel._loop,
        )

    def unsubscribe(self, endpoint: str, args):
        return asyncio.run_coroutine_threadsafe(
            self._connection(endpoint).unsubscribe(args), self.kernel._loop,
        )

    def health(self) -> dict:
        return {
            "public": self.public.health(),
            "business": self.business.health(),
            "private": self.private.health() if self.private else None,
            "algo": self.algo.health() if self.algo else None,
            "writer": self.writer.stats(),
        }

    def stop(self):
        for connection in (self.public, self.business, self.private, self.algo):
            if connection is not None:
                connection.active = False
        for task in self._tasks:
            task.cancel()
        self.writer.stop()
