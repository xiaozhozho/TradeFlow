"""动态订阅与健康指标测试（不起真实 WS 连接）。"""
import asyncio
import threading
import time

import pytest

from tradeflow.kernel import Kernel
from tradeflow.subscribe.manager import SubscribeManager


class FakeDolphin:
    def __init__(self, *args, **kwargs):
        pass


class FakeWriter:
    def __init__(self, *args, **kwargs):
        self._stats = {"queue": 0, "written": 0, "dropped": 0, "errors": 0}

    def stats(self):
        return dict(self._stats)

    def stop(self):
        pass


@pytest.fixture
def kernel(monkeypatch):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    monkeypatch.setattr("tradeflow.subscribe.manager.StreamWriter", FakeWriter)
    k = Kernel(flag="1", private=False, instIds={"BTC-USDT-SWAP"})
    thread = threading.Thread(
        target=lambda: (asyncio.set_event_loop(k._loop), k._loop.run_forever()),
        daemon=True,
    )
    thread.start()
    for _ in range(50):
        if k._loop.is_running():
            break
        time.sleep(0.05)
    yield k
    k._loop.call_soon_threadsafe(k._loop.stop)
    thread.join(timeout=2)


@pytest.fixture
def manager(kernel):
    m = SubscribeManager(kernel)
    kernel.manager = m
    return m


def test_manager_dynamic_subscribe(manager):
    future = manager.subscribe("public", [{"channel": "tickers", "instId": "ETH-USDT-SWAP"}])
    future.result(timeout=2)
    assert any("ETH-USDT-SWAP" in key for key in manager.public.desired)
    # health 反映新频道
    health = manager.health()
    assert "tickers:ETH-USDT-SWAP" in health["public"]["channels"]


def test_manager_dynamic_unsubscribe(manager):
    manager.subscribe("public", [{"channel": "tickers", "instId": "ETH-USDT-SWAP"}]).result(timeout=2)
    manager.unsubscribe(
        "public", [{"channel": "tickers", "instId": "ETH-USDT-SWAP"}],
    ).result(timeout=2)
    assert not any("ETH-USDT-SWAP" in key for key in manager.public.desired)
    assert "tickers:BTC-USDT-SWAP" in manager.health()["public"]["channels"]


def test_manager_health_shape(manager):
    health = manager.health()
    assert set(health) == {"public", "business", "private", "algo", "writer"}
    for endpoint in ("public", "business"):
        assert health[endpoint]["connected"] is False
        assert health[endpoint]["reconnect_count"] == 0
        assert isinstance(health[endpoint]["channels"], list)
    assert health["writer"]["written"] == 0
    # Kernel.health 透传
    assert manager.kernel.health() == health


def test_manager_subscribe_disabled_endpoint_raises(manager):
    with pytest.raises(RuntimeError):
        manager.subscribe("private", [{"channel": "account"}])
