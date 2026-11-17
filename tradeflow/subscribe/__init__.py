"""OKX WebSocket 行情订阅模块。

结构：
- ``base.py``     连接/心跳/重连/动态订阅基类
- ``channels.py`` 频道清单唯一来源（端点、参数、写入表、默认开关）
- ``handlers/``   packet -> Batch 纯函数解析
- ``public.py``   公共行情与业务端点连接
- ``private.py``  私有连接（需登录）与策略委托连接
- ``manager.py``  多连接编排 + 共享 StreamWriter
- ``writer.py``   后台批量写 DolphinDB
- ``reconcile.py`` 断线 REST 对账 + K 线缺口补齐

兼容性：``PublicSubscribe / CandleSubscribe / PrivateSubscribe`` 等旧入口
继续从这里导出；``Subscribe`` 保留为 ``base.Subscribe`` 的旧名别名
（计划 3.7 移除），新代码请使用 ``BaseSubscribe``。
"""
from .base import Subscribe  # noqa: F401  (旧名兼容别名，3.7 移除)
from .base import Subscribe as BaseSubscribe  # noqa: F401
from .public import CandleSubscribe, PublicSubscribe  # noqa: F401
from .private import BusinessPrivateSubscribe, PrivateSubscribe  # noqa: F401
from .writer import StreamWriter  # noqa: F401
from .manager import SubscribeManager  # noqa: F401
from .channels import (  # noqa: F401
    CHANNEL_SPECS,
    DEFAULT_ALGO_CHANNELS,
    DEFAULT_CANDLE_CHANNELS,
    DEFAULT_MARKET_CHANNELS,
    DEFAULT_PRIVATE_CHANNELS,
    ChannelSpec,
    get_channel_spec,
    tables_for,
    to_index_id,
)

__all__ = [
    # 连接类
    "PublicSubscribe", "CandleSubscribe", "PrivateSubscribe",
    "BusinessPrivateSubscribe", "Subscribe", "BaseSubscribe",
    # 编排
    "SubscribeManager", "StreamWriter",
    # 频道
    "CHANNEL_SPECS", "ChannelSpec", "get_channel_spec", "tables_for",
    "DEFAULT_MARKET_CHANNELS", "DEFAULT_CANDLE_CHANNELS",
    "DEFAULT_PRIVATE_CHANNELS", "DEFAULT_ALGO_CHANNELS",
]
