"""StreamWriter 异步批量写入测试。"""
import threading

import pandas as pd

from tradeflow.subscribe.writer import StreamWriter


class FakeSession:
    """可阻塞的假 session：block_first=True 时首次写入挂起，模拟写库慢。"""

    def __init__(self, block_first: bool = False):
        self.calls = []
        self.block_first = block_first
        self.first_write_started = threading.Event()
        self.release = threading.Event()

    def run(self, script, frame):
        self.calls.append((script, frame))
        if self.block_first and not self.release.is_set():
            self.first_write_started.set()
            self.release.wait(timeout=5)


class FakeDolphin:
    def __init__(self, block_first: bool = False):
        self.session = FakeSession(block_first)

    def newSession(self):
        return self.session


def test_writer_batches_by_table():
    dolphin = FakeDolphin()
    # flush_interval 加大到 0.2s，保证两帧在同一积累窗口内合并（避免调度抖动拆批）
    writer = StreamWriter(dolphin, batch_size=10, flush_interval=0.2)
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 1}]))
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 2}]))
    writer.stop()
    assert len(dolphin.session.calls) == 1
    assert len(dolphin.session.calls[0][1]) == 2


def test_writer_groups_multiple_tables():
    dolphin = FakeDolphin()
    writer = StreamWriter(dolphin, batch_size=10, flush_interval=0.2)
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 1}]))
    writer.put("candleStreamTable", pd.DataFrame([{"ts": 1, "c": 1.0}]))
    writer.stop()
    scripts = [script for script, _ in dolphin.session.calls]
    assert len(scripts) == 2
    assert any("tickStreamTable" in s for s in scripts)
    assert any("candleStreamTable" in s for s in scripts)


def test_writer_drops_when_full():
    dolphin = FakeDolphin(block_first=True)
    writer = StreamWriter(dolphin, batch_size=1, flush_interval=1, queue_size=1)
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 1}]))
    # 等写线程取走第一帧并阻塞在 session.run 内，此时队列已空且无法继续消化
    assert dolphin.session.first_write_started.wait(timeout=2)
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 2}]))  # 占满队列
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 3}]))  # 必然丢弃
    assert writer.stats()["dropped"] >= 1
    dolphin.session.release.set()  # 放行写线程，恢复队列消化
    writer.stop()


def test_writer_skips_empty_frame():
    dolphin = FakeDolphin()
    writer = StreamWriter(dolphin, batch_size=10, flush_interval=0.2)
    writer.put("tickStreamTable", pd.DataFrame())
    writer.stop()
    assert dolphin.session.calls == []


def test_writer_records_written_rows():
    dolphin = FakeDolphin()
    writer = StreamWriter(dolphin, batch_size=10, flush_interval=0.2)
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 1}]))
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "ETH", "ts": 2}]))
    writer.stop()
    assert writer.stats()["written"] == 2
    assert writer.stats()["dropped"] == 0
