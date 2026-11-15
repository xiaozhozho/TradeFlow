# TradeFlow

Python 量化交易框架，以第三方包形式提供，供量化策略开发时导入使用。

## 安装

```bash
# 在项目根目录（本文件所在目录）执行
pip install -e .
```

也可以不安装，直接把项目根目录加入 `PYTHONPATH`：

```bash
export PYTHONPATH=/path/to/TradeFlow:$PYTHONPATH
```

## 快速开始

```python
from tradeflow import Kernel
from tradeflow.strategy import Strategy


class MyStrategy(Strategy):
    instIds = {"BTC-USDT-SWAP"}

    def preinit(self): ...
    def init(self): ...
    def scan(self): ...


kernel = Kernel(flag="1")  # flag="1" 模拟盘，"0" 实盘
kernel.addStrategy(MyStrategy)
kernel.start()
```

完整示例见 `strategy/example.py`，在项目根目录运行：

```bash
python -m strategy.example
```

`Kernel` 常用参数：`warmup` 为启动预热秒数（默认 320），`private=False` 时只订阅公共行情、无需私有 API Key。

## 目录结构

```
TradeFlow/
├── tradeflow/              # 框架包（发布为第三方包 tradeflow）
│   ├── okx/                # OKX SDK（同步上游 python-okx）
│   ├── stream/             # DolphinDB 连接、流表与脚本
│   ├── utils/              # 日志、网络、邮件等工具
│   ├── kernel.py           # 核心调度组件
│   ├── strategy.py         # 策略基类
│   ├── subscribe.py        # WebSocket 行情订阅
│   ├── risk.py             # 风险管理
│   ├── indicator.py        # 指标计算
│   └── setting.py          # 全局配置
├── strategy/               # 量化策略工作区（不随包发布）
│   ├── example.py          # 策略模板
│   └── README.md
├── .env                    # 本地密钥（已忽略，不提交）
├── .env.example            # 环境变量模板
├── pyproject.toml          # 打包配置
├── requirements.txt
└── README.md
```

## 配置

- `.env` 查找顺序：`TRADEFLOW_ENV_FILE` 指定路径 → 当前工作目录 → 包目录 → 源码仓库根目录；
- 真实环境变量优先级最高，`.env` 不会覆盖已存在的环境变量；
- 配置模板见 `.env.example`，复制为 `.env` 后填写；
- 日志默认写入调用脚本同目录的 `log/` 下，文件名为脚本名（如 `test.py` → `log/test.log`）；可用 `TRADEFLOW_LOG_DIR` 覆盖日志根目录。

## 版本记录

## version 1.0.0

- 添加基本组件：log, utils, setting, decorator
- 封装okx为subscribe， 包括public、private订阅；
- 编写公共组件，包括database,  utils, setting 。



## version 2.0.0

- 添加创建流数据表的脚本、函数功能；
- 为PublicSubscribe, PrivatePublic 添加callback函数，将订阅的数据插入流数据表；
- 添加TradeFlow的核心组件kernel；
- 添加TradeFlow的核心组件----策略模版；
- 修改订阅形式， 将public、private订阅函数使用asyncio封装成异步订阅。



## version 2.5.0

- 修改TradeFlow模块文件结构；
- 修改public、private异步订阅, 将其写入独立的子线程，守护线程开启；
- 修改dolphin.py, 将功能封装进Dolphin类，进行统一管理，添加kernel绑定；
- 添加mysql.py 到database模块；
- 添加email.py 到utils模块；
- 添加chart.py 到utils模块

## version 2.6.0

- 设计新的内存数据表: 其中shareTickTable用于保存最新tick数据, shareAccountTable用于保存账户最新信息, sharePositionTable用于保存最新持仓情况


## version 3.0.0

- 添加risk 模块用于全局风险控制，重要功能包括，调节持仓模式、保证金模式、杠杆、亏损时按照市价平仓等


## version 3.1.0

- 将RiskManage 添加进主进程当中,而非与subscribe处于相同的子线程, 提高线程安全.

## version 3.1.1

- 修复log.py 在Linux 和 win32 效果差异不同的问题

## version 3.2.0

- okx 模块同步上游 python-okx 0.4.4（commit 06bb8a9），保留目录名 `okx`；
- 新增 `okx/api.py` 聚合层，适配上游拆分式 API 类（新增 Affiliate/CopyTrading/DualInvest/SpreadTrading）；
- WebSocket 地址迁移至 443 端口（OKX 已于 2026-10-31 停用 8443），并处理 `notice` 服务通知；
- 修复 pandas 3 兼容性：`applymap` 替换为 `dfApply`，避免字符串 dtype 下 `iloc` 回写数值报错；
- 新增 `OKX_FORCE_IPV4` 开关（默认开启，仅对 OKX 域名使用 IPv4；IPv6 正常的环境可设为 `0` 关闭）；
- 日志目录改为相对项目根目录并自动创建，不再依赖启动时的工作目录。

## version 3.3.0

- 包代码统一迁移到 `tradeflow/` 目录，可通过 `pip install -e .` 以第三方包导入；
- 新增 `strategy/` 量化策略开发目录与最小策略模板 `strategy/example.py`；
- 新增 `pyproject.toml` 打包配置与 `.env.example` 环境变量模板；
- `.env` 支持从当前目录、包目录、仓库根目录查找，并可用 `TRADEFLOW_ENV_FILE` 指定；
- 日志目录支持 `TRADEFLOW_LOG_DIR` 覆盖，源码模式下仍写入仓库根目录 `log/`。

## version 3.3.1

- 日志改为按调用脚本定位：在脚本同目录创建 `log/`，并写入与脚本同名的日志文件（如 `test.py` → `log/test.log`）；
- `get_logger` 支持从 `tradeflow` 顶层直接导入；
- `TRADEFLOW_LOG_DIR` 仍可用于覆盖日志根目录。

## version 3.3.2

- `Kernel` 新增 `warmup`（启动预热秒数）与 `private`（是否订阅私有频道）参数，支持仅公共行情运行；
- 修复 `Strategy.run()` 重复执行 `init()` 的问题，策略初始化只执行一次；
- WebSocket 消息解析由 `eval` 改为 `json.loads`，避免 JSON 布尔值导致断线重连；
- 私有频道数值字段统一使用 `pd.to_numeric` 容错（空串、`unsupported`、`off` 等按 0 处理），并按账户表结构对齐列；
- `Kernel.stop()` 取消订阅协程并优雅关闭 WebSocket；
- `strategy/example.py` 跑通完整示例：创建 DolphinDB 流表/引擎、订阅 OKX tick 并落库、每 3 秒统计 `tickStreamTable` 数据量。
