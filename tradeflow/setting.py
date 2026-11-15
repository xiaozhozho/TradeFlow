"""全局配置。

所有敏感信息（API Key / Secret / 密码 / 邮箱授权码）统一放在 `.env` 文件中，
由 `_find_env_file` 按优先级查找（当前目录、包目录、仓库根目录）；`.env` 已在
`.gitignore` 中排除。

优先级：真实环境变量 > .env 中的值 > 代码内默认值（默认值为空，缺失时启动即报错）。
"""
import os
from pathlib import Path


def _find_env_file() -> Path | None:
    """按优先级查找 .env：显式路径 > 当前工作目录 > 包目录 > 源码仓库根目录。"""
    explicit = os.getenv("TRADEFLOW_ENV_FILE")
    package_dir = Path(__file__).resolve().parent
    candidates = [
        Path(explicit).expanduser() if explicit else None,
        Path.cwd() / ".env",
        package_dir / ".env",
        package_dir.parent / ".env",
    ]
    for candidate in candidates:
        if candidate is not None and candidate.is_file():
            return candidate
    return None


def _load_env() -> None:
    """将 .env 文件内容写入环境变量；已存在的环境变量不会被覆盖。"""
    env_file = _find_env_file()
    if env_file is None:
        return
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        # 仅去掉成对的引号，避免破坏密码本身含有的引号字符
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        os.environ.setdefault(key, value)


_load_env()

# OKX
OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_SECRETE_KEY = os.getenv("OKX_SECRETE_KEY", "")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")

# OKX SIMULATE
OKX_API_KEY_SIMULATE = os.getenv("OKX_API_KEY_SIMULATE", "")
OKX_SECRETE_KEY_SIMULATE = os.getenv("OKX_SECRETE_KEY_SIMULATE", "")
OKX_PASSPHRASE_SIMULATE = os.getenv("OKX_PASSPHRASE_SIMULATE", "")

# OKX WebSocket 地址
# 上游 python-okx 只维护 REST 常量，WS 地址由本项目维护。
# flag='0' 使用实盘地址，flag='1' 使用模拟盘地址。
# 注意：OKX 已于 2026-10-31 停用 8443 端口，这里统一使用默认的 443 端口。
REAL_PUBLIC_URL = os.getenv("OKX_WS_REAL_PUBLIC_URL", "wss://ws.okx.com/ws/v5/public")
REAL_CANDLE_URL = os.getenv("OKX_WS_REAL_CANDLE_URL", "wss://ws.okx.com/ws/v5/business")
REAL_PRIVATE_URL = os.getenv("OKX_WS_REAL_PRIVATE_URL", "wss://ws.okx.com/ws/v5/private")

SIMULATE_PUBLIC_URL = os.getenv(
    "OKX_WS_SIMULATE_PUBLIC_URL", "wss://wspap.okx.com/ws/v5/public"
)
SIMULATE_CANDLE_URL = os.getenv(
    "OKX_WS_SIMULATE_CANDLE_URL", "wss://wspap.okx.com/ws/v5/business"
)
SIMULATE_PRIVATE_URL = os.getenv(
    "OKX_WS_SIMULATE_PRIVATE_URL", "wss://wspap.okx.com/ws/v5/private"
)

# 网络：部分 DNS/代理会返回不可用的 IPv6 映射地址。
# 默认仅对 OKX 域名使用 IPv4；如所在网络仅支持 IPv6，可设 OKX_FORCE_IPV4=0 关闭。
OKX_FORCE_IPV4 = os.getenv("OKX_FORCE_IPV4", "1").lower() not in ("0", "false", "no")


# DolphinDB
DOLPHINDB_HOST = os.getenv("DOLPHINDB_HOST", "localhost")
DOLPHINDB_PORT = int(os.getenv("DOLPHINDB_PORT", "8848"))
DOLPHINDB_USER = os.getenv("DOLPHINDB_USER", "admin")
DOLPHINDB_PASSWORD = os.getenv("DOLPHINDB_PASSWORD", "")

# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "")


# Email
EMAIL_SERVER = os.getenv("EMAIL_SERVER", "smtp.qq.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "465"))
EMAIL_USERNAME = os.getenv("EMAIL_USERNAME", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "")
EMAIL_REVEIVER = os.getenv("EMAIL_REVEIVER", "")


# global setting
TIMEOUT = int(os.getenv("TIMEOUT", "999999"))

# stop max loss and profit
MAX_LOSS = float(os.getenv("MAX_LOSS", "0.3"))
MAX_PROFIT = float(os.getenv("MAX_PROFIT", "999"))

# lever
LEVER = int(os.getenv("LEVER", "30"))
