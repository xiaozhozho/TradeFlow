"""DFS 历史归档集成测试（写入但不删除流表，仅需 TRADEFLOW_TEST_DDB=1）。"""
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("TRADEFLOW_TEST_DDB") != "1",
    reason="需要本地 DolphinDB",
)


def test_archive_subscription_created():
    from tradeflow.stream.archive import ensure_archive
    from tradeflow.stream.dolphin import Dolphin
    from tradeflow.stream.schema import STREAM_TABLES

    d = Dolphin()
    # 源流表可能被其他集成测试的 teardown 删除，这里幂等补建（存在则跳过）
    d.prepare_streams(["tickStreamTable", "tradeStreamTable"])
    ensure_archive(d)
    workers = d.runScript("getStreamingStat().subWorkers")
    assert any("archiveTick" in str(row) for row in workers.to_dict("records"))


def test_archive_writes_to_dfs():
    from tradeflow.stream.archive import ARCHIVE_DB, ensure_archive
    from tradeflow.stream.dolphin import Dolphin
    from tradeflow.stream.schema import STREAM_TABLES

    d = Dolphin()
    d.prepare_streams(["tickStreamTable", "tradeStreamTable"])
    ensure_archive(d)
    # 直接调用归档 handler 验证转换逻辑，不向流表写测试数据
    d.runScript("""
    msg = table(2030.12.31T00:00:00.000 as ts, "ZZ-ARCHIVE-TEST" as instId, 100.0 as last, 1.0 as lastSz)
    archiveTick(msg)
    """)
    count = d.runScript(
        f'exec count(*) from loadTable("{ARCHIVE_DB}", `tick) '
        'where date = 2030.12.31 and instId = "ZZ-ARCHIVE-TEST"'
    )
    assert count == 1
    d.runScript(
        f'delete from loadTable("{ARCHIVE_DB}", `tick) '
        'where date = 2030.12.31 and instId = "ZZ-ARCHIVE-TEST"'
    )
