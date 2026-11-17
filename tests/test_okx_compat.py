"""OKX vendored 模块兼容性修复测试（Python 3.12 / httpx 新版告警）。"""
import datetime
import warnings

from tradeflow.okx import utils
from tradeflow.okx.api import Api
from tradeflow.okx.okxclient import OkxClient


def test_get_timestamp_is_utc_z_format_without_deprecation():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        timestamp = utils.get_timestamp()
    assert timestamp.endswith("Z")
    assert "+" not in timestamp
    parsed = datetime.datetime.fromisoformat(timestamp.replace("Z", ""))
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    assert abs((now - parsed).total_seconds()) < 300


class ProbeClient(OkxClient):
    """拦截 post 调用，验证 _request 传给 httpx 的 kwarg。"""

    def __init__(self):
        super().__init__(flag="1")
        self.captured = {}

    def post(self, url, **kwargs):
        self.captured = {"url": url, **kwargs}

        class Response:
            status_code = 200

            @staticmethod
            def json():
                return {"code": "0", "data": []}

        return Response()


def test_post_request_uses_content_kwarg():
    client = ProbeClient()
    client._request("POST", "/api/v5/test", {"a": 1})
    # httpx 已废弃 data=<str/bytes>，必须用 content=
    assert client.captured.get("content") == '{"a": 1}'
    assert "data" not in client.captured


def test_api_construction_has_no_deprecation_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        Api(flag="1")
