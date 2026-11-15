"""行情异步批量落库。

独立线程 + 独立 DolphinDB session：

- WebSocket 事件循环只调用 ``put()``（非阻塞入队），写库延迟不反压行情；
- 按表分组批量 ``tableInsert``，写入前统一走 ``TableSpec.normalize``；
- 批量失败按 100 行分块重试，一条脏数据不会毁掉整批；
- 队列满时丢弃并计数告警，宁可丢数据也不阻塞行情连接。
"""
import queue
import threading
import time
from collections import defaultdict

import pandas as pd

from ..stream.schema import STREAM_TABLES
from ..utils.log import get_logger

logger = get_logger()

_CHUNK_SIZE = 100


class StreamWriter:
    """按表批量写 DolphinDB 流表的后台写线程。"""

    def __init__(self, dolphin, batch_size: int = 200, flush_interval: float = 0.2,
                 queue_size: int = 200_000):
        self._dolphin = dolphin
        self._session = dolphin.newSession()
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name="StreamWriter", daemon=True,
        )
        self._written = 0
        self._dropped = 0
        self._errors = 0
        self._last_drop_log = 0
        self._thread.start()

    # ---------- 对外接口 ----------

    def put(self, table: str, frame: pd.DataFrame) -> None:
        """非阻塞入队；队列满时丢弃当前帧并计数。"""
        if frame is None or frame.empty:
            return
        try:
            self._queue.put_nowait((table, frame))
        except queue.Full:
            self._dropped += len(frame)
            if self._dropped - self._last_drop_log >= 5000:
                logger.error(f"写队列已满，累计丢弃 {self._dropped} 行（最新丢弃表 {table}）")
                self._last_drop_log = self._dropped

    def stats(self) -> dict:
        return {
            "queue": self._queue.qsize(),
            "written": self._written,
            "dropped": self._dropped,
            "errors": self._errors,
        }

    def stop(self, timeout: float = 5.0) -> None:
        """停止写线程；先排空队列再退出。"""
        self._stop.set()
        self._thread.join(timeout=timeout)

    # ---------- 内部实现 ----------

    def _loop(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            batch = []
            rows = 0
            try:
                item = self._queue.get(timeout=self._flush_interval)
                batch.append(item)
                rows += len(item[1])
            except queue.Empty:
                continue
            deadline = time.monotonic() + self._flush_interval
            while rows < self._batch_size:
                timeout = max(0.0, deadline - time.monotonic())
                try:
                    item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    break
                batch.append(item)
                rows += len(item[1])
            self._flush(batch)

    def _flush(self, batch) -> None:
        groups: dict[str, list[pd.DataFrame]] = defaultdict(list)
        for table, frame in batch:
            groups[table].append(frame)
        for table, frames in groups.items():
            try:
                frame = pd.concat(frames, ignore_index=True)
            except Exception as exc:
                self._errors += sum(len(f) for f in frames)
                logger.exception(f"合并批次失败 {table}: {exc}")
                continue
            spec = STREAM_TABLES.get(table)
            if spec is None:
                self._errors += len(frame)
                logger.error(f"未知流表 {table}，丢弃 {len(frame)} 行")
                continue
            try:
                frame = spec.normalize(frame)
            except Exception as exc:
                self._errors += len(frame)
                logger.exception(f"规范化 {table} 失败，丢弃 {len(frame)} 行: {exc}")
                continue
            self._insert(table, frame)

    def _insert(self, table: str, frame: pd.DataFrame) -> None:
        try:
            self._session.run(f"tableInsert{{{table}}}", frame)
            self._written += len(frame)
            return
        except Exception as exc:
            logger.exception(f"写入 {table} 失败: {exc}")
        # 整批失败时按块重试，跳过脏数据块
        for start in range(0, len(frame), _CHUNK_SIZE):
            chunk = frame.iloc[start:start + _CHUNK_SIZE]
            try:
                self._session.run(f"tableInsert{{{table}}}", chunk)
                self._written += len(chunk)
            except Exception as chunk_exc:
                self._errors += len(chunk)
                logger.error(f"{table} 分块重试仍失败: {chunk_exc}")
