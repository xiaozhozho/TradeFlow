"""网络兼容工具。"""
import socket

_PATCHED = False


def prefer_ipv4_for_okx() -> None:
    """让 OKX 域名只使用 IPv4 解析（幂等）。

    部分代理/DNS 环境会同时返回 IPv4 和不可用的 IPv6 映射地址，
    Python 的 httpx / websockets 不会像浏览器那样快速回退，导致连接超时。
    仅在 setting.OKX_FORCE_IPV4 开启时调用，其他域名不受影响。
    """
    global _PATCHED
    if _PATCHED:
        return

    original_getaddrinfo = socket.getaddrinfo

    def ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
        if isinstance(host, str) and (host == "okx.com" or host.endswith(".okx.com")):
            family = socket.AF_INET
        return original_getaddrinfo(host, port, family, type, proto, flags)

    socket.getaddrinfo = ipv4_only
    _PATCHED = True
