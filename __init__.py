"""
Author: Mr:xzz
version: 3.1.1
"""

__version__ = '3.1.1'

from .setting import OKX_FORCE_IPV4

if OKX_FORCE_IPV4:
    from .utils.net import prefer_ipv4_for_okx

    prefer_ipv4_for_okx()