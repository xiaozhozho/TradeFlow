"""
TradeFlow：Python 量化交易框架，可作为第三方包导入使用。

Author: Mr:xzz
version: 3.3.2
"""

__version__ = "3.3.2"

from .setting import OKX_FORCE_IPV4

if OKX_FORCE_IPV4:
    from .utils.net import prefer_ipv4_for_okx

    prefer_ipv4_for_okx()

__all__ = ["Kernel", "Strategy", "get_logger", "__version__"]


def __getattr__(name: str):
    """按需导入重型组件，避免 `import tradeflow` 时加载全部依赖。"""
    if name == "Kernel":
        from .kernel import Kernel

        return Kernel
    if name == "Strategy":
        from .strategy import Strategy

        return Strategy
    if name == "get_logger":
        from .utils.log import get_logger

        return get_logger
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
