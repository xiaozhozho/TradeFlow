"""OKX WebSocket 行情订阅模块。

结构：
- ``base.py``     连接/心跳/重连/动态订阅基类
- ``channels.py`` 频道清单唯一来源（端点、参数、写入表、默认开关）
- ``handlers/``   packet -> Batch 纯函数解析
- ``public.py``   公共行情与业务端点连接
- ``private.py``  私有连接（需登录）与策略委托连接
- ``manager.py``  多连接编排 + 共享 StreamWriter
- ``writer.py``   后台批量写 DolphinDB
- ``legacy.py``   旧实现（兼容保留，逐步退役）

兼容性：``PublicSubscribe / CandleSubscribe / PrivateSubscribe`` 等旧入口
继续从这里导出，现在指向新实现；``Subscribe`` 仍指向旧基类以兼容
``isinstance`` 等潜在用法，新基类以 ``BaseSubscribe`` 导出。
"""
from .legacy import ACCOUNT_COLUMNS, Subscribe, toNumeric  # noqa: F401
from .base import Subscribe as BaseSubscribe  # noqa: F401
from .public import CandleSubscribe, PublicSubscribe  # noqa: F401  (覆盖 legacy)
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
    # 兼容旧接口
    "ACCOUNT_COLUMNS", "toNumeric",
]
