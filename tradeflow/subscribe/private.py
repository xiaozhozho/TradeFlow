"""私有连接：/ws/v5/private 与业务端点私有频道（策略委托）。"""
import base64
import hmac
import json
import time

from ..setting import (
    OKX_API_KEY, OKX_API_KEY_SIMULATE,
    OKX_PASSPHRASE, OKX_PASSPHRASE_SIMULATE,
    OKX_SECRETE_KEY, OKX_SECRETE_KEY_SIMULATE,
    REAL_CANDLE_URL, REAL_PRIVATE_URL,
    SIMULATE_CANDLE_URL, SIMULATE_PRIVATE_URL,
)
from .base import Subscribe


class PrivateSubscribe(Subscribe):
    """账户/持仓/订单/成交/爆仓预警/Greeks。"""

    endpoint = "private"
    login_required = True

    def default_url(self) -> str:
        return REAL_PRIVATE_URL if self.flag == "0" else SIMULATE_PRIVATE_URL

    @property
    def apiKey(self) -> str:
        return OKX_API_KEY if self.flag == "0" else OKX_API_KEY_SIMULATE

    @property
    def passPhrase(self) -> str:
        return OKX_PASSPHRASE if self.flag == "0" else OKX_PASSPHRASE_SIMULATE

    @property
    def secretKey(self) -> str:
        return OKX_SECRETE_KEY if self.flag == "0" else OKX_SECRETE_KEY_SIMULATE

    @property
    def loginParam(self) -> str:
        """OKX WS 登录参数（HMAC-SHA256 签名）。"""
        timestamp = str(int(time.time()))
        message = timestamp + "GET" + "/users/self/verify"
        mac = hmac.new(
            bytes(self.secretKey, encoding="utf-8"),
            bytes(message, encoding="utf-8"),
            digestmod="sha256",
        )
        sign = base64.b64encode(mac.digest()).decode("utf-8")
        return json.dumps({
            "op": "login",
            "args": [{
                "apiKey": self.apiKey,
                "passphrase": self.passPhrase,
                "timestamp": timestamp,
                "sign": sign,
            }],
        })


class BusinessPrivateSubscribe(PrivateSubscribe):
    """业务端点私有频道：orders-algo / algo-advance（策略委托推送）。"""

    endpoint = "business-private"

    def default_url(self) -> str:
        return REAL_CANDLE_URL if self.flag == "0" else SIMULATE_CANDLE_URL
