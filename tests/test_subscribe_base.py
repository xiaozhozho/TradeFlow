"""WebSocket 连接基类测试（离线：不发网络请求）。"""
import pytest

from tradeflow.subscribe.base import Subscribe


class Dummy(Subscribe):
    endpoint = "public"
    login_required = False

    def default_url(self):
        return "wss://example.invalid"


def make_sub(channels=None):
    return Dummy(None, channels or [{"channel": "tickers", "instId": "BTC-USDT-SWAP"}], writer=None)


def test_dynamic_subscribe_updates_desired():
    sub = make_sub()
    assert len(sub.desired) == 1
    sub.add_desired([{"channel": "tickers", "instId": "ETH-USDT-SWAP"}])
    assert len(sub.desired) == 2
    # 重复添加幂等
    sub.add_desired([{"channel": "tickers", "instId": "ETH-USDT-SWAP"}])
    assert len(sub.desired) == 2


def test_remove_desired():
    sub = make_sub()
    sub.remove_desired([{"channel": "tickers", "instId": "BTC-USDT-SWAP"}])
    assert len(sub.desired) == 0


def test_error_event_isolated():
    sub = make_sub([{"channel": "books5", "instId": "BTC-USDT-SWAP"}])
    sub._handle_event({
        "event": "error", "code": "60012",
        "msg": "channel does not exist",
        "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"},
    })
    # 频道错误只移除该频道，不影响连接
    assert len(sub.desired) == 0
    assert sub.error_count == 1
    assert sub.reconnect_count == 0


def test_connection_level_error_raises():
    sub = make_sub()
    with pytest.raises(RuntimeError):
        sub._handle_event({
            "event": "error", "code": "60009", "msg": "login failed", "arg": {},
        })


def test_notice_and_subscribe_events_logged_not_fatal():
    sub = make_sub()
    sub._handle_event({"event": "notice", "code": "64008", "msg": "upgrade"})
    sub._handle_event({"event": "subscribe", "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"}})
    assert sub.error_count == 0


def test_health_shape():
    sub = make_sub()
    health = sub.health()
    assert health["connected"] is False
    assert health["channels"] == ["tickers:BTC-USDT-SWAP"]
    assert health["reconnect_count"] == 0
    assert health["error_count"] == 0
    assert health["endpoint"] == "public"


def test_key_is_canonical():
    sub = make_sub()
    assert sub._key({"channel": "tickers", "instId": "A"}) == sub._key({"instId": "A", "channel": "tickers"})
