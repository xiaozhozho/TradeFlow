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

`Kernel` 常用参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `warmup` | 320 | 启动预热秒数（等待行情/订阅就绪） |
| `private` | True | False 时只订阅公共行情，无需私有 API Key |
| `market` | None | 公共行情频道集合，None 用默认 9 个频道 |
| `depth` | None | 盘口频道：`"books5"` / `"bbo-tbt"` / `"books"` |
| `options` | False | 订阅期权频道（opt-summary / option-trades / estimated-price） |
| `algo` | False | 订阅策略委托频道（orders-algo / algo-advance） |
| `recreate_tables` | False | prepare() 时删除重建全部流表（**会清空流表数据**，升级/开发用） |

动态订阅（不重启进程）：

```python
kernel.manager.subscribe("public", [{"channel": "tickers", "instId": "ETH-USDT-SWAP"}])
kernel.manager.unsubscribe("public", [{"channel": "tickers", "instId": "ETH-USDT-SWAP"}])
```

## 目录结构

```
TradeFlow/
├── tradeflow/                  # 框架包（发布为第三方包 tradeflow）
│   ├── okx/                    # OKX SDK（同步上游 python-okx）
│   ├── subscribe/              # WebSocket 行情订阅模块
│   │   ├── base.py             # 连接/心跳/重连/动态订阅基类
│   │   ├── channels.py         # 频道清单唯一来源（端点/参数/写入表/默认开关）
│   │   ├── handlers/           # 报文解析（packet -> Batch 纯函数）
│   │   ├── public.py           # 公共行情与业务端点连接
│   │   ├── private.py          # 私有连接（需登录）与策略委托连接
│   │   ├── manager.py          # 多连接编排 + 共享 StreamWriter
│   │   ├── writer.py           # 后台批量写 DolphinDB
│   │   ├── reconcile.py        # 断线 REST 对账 + K 线缺口补齐
│   │   ├── orderbook.py        # 增量盘口与序号校验（books）
│   │   └── legacy.py           # 旧实现（兼容保留）
│   ├── stream/                 # DolphinDB 连接、表结构与引擎
│   │   ├── schema.py           # 流表/快照表结构唯一来源（TableSpec）
│   │   ├── engines.py          # 流引擎定义（快照/响应式状态/时间序列）
│   │   ├── archive.py          # 流表 -> DFS 历史归档（P1）
│   │   ├── script.py           # 建表脚本（由 schema.py 生成）
│   │   └── dolphin.py          # DolphinDB 连接与执行
│   ├── utils/                  # 日志、网络、邮件、数据格式化工具
│   │   └── format.py           # 订阅数据格式化纯函数（时间/空串/列对齐）
│   ├── kernel.py               # 核心调度组件
│   ├── strategy.py             # 策略基类
│   ├── risk.py                 # 风险管理
│   ├── indicator.py            # 指标计算
│   └── setting.py              # 全局配置
├── strategy/                   # 量化策略工作区（不随包发布）
│   ├── example.py              # 策略模板
│   └── README.md
├── tests/                      # 单元测试 + DolphinDB 集成测试
├── .env                        # 本地密钥（已忽略，不提交）
├── .env.example                # 环境变量模板
├── pyproject.toml              # 打包配置
├── requirements.txt
└── README.md
```

## 频道矩阵

频道清单唯一来源：`tradeflow/subscribe/channels.py`。端点：`public` = `/ws/v5/public`，
`business` = `/ws/v5/business`，`private` = `/ws/v5/private`（需登录），
`business-private` = 业务端点私有频道（需登录）。

| 频道 | 端点 | 写入表 | 默认 |
|---|---|---|---|
| `tickers` | public | `tickStreamTable` | 开 |
| `trades` | public | `tradeStreamTable` | 开 |
| `open-interest` | public | `openinterestStreamTable` | 开 |
| `funding-rate` | public | `fundingrateStreamTable` | 开 |
| `mark-price` | public | `markpriceStreamTable` | 开 |
| `index-tickers` | public | `indextickStreamTable` | 开 |
| `liquidation-orders` | public | `liquidationorderStreamTable` | 开 |
| `status` | public | `statusStreamTable` | 开 |
| `instruments` | public | `productStreamTable` | 开 |
| `books5` / `bbo-tbt` / `books` | public | `bookStreamTable` | 关（`depth=` 开启） |
| `price-limit` | public | `pricelimitStreamTable` | 关 |
| `opt-summary` / `option-trades` / `estimated-price` | public | 对应流表 | 关（`options=True`） |
| `candle1m` / `candle5m` | business | `candleStreamTable` + `candleConfirmStreamTable` | 开 |
| `mark-price-candle*` / `index-candle*` / `trades-all` | business | 对应流表 | 关 |
| `account` / `positions` / `orders` / `balance_and_position` / `liquidation-warning` / `account-greeks` | private | 对应快照/流表 | 开（`private=True`） |
| `fills` | private | `fillsStreamTable` | 关（VIP4+） |
| `orders-algo` / `algo-advance` | business-private | `algoOrderStreamTable` + `shareAlgoOrderTable` | 关（`algo=True`） |

## 流数据表与流引擎

- 表结构唯一来源：`tradeflow/stream/schema.py`（`TableSpec`）；旧表列顺序不变，新列追加末尾；
- 快照表（`latestIndexedTable`）：`shareTickTable`、`shareCandleTable`、`shareOrderTable`、
  `shareAlgoOrderTable`、`shareAccountTable`、`sharePositionTable`、`shareBalanceTable`、
  `shareWarningTable`、`shareAccountGreeksTable`、`shareCandleIndicatorTable`、`shareTickIndicatorTable`；
- K 线指标引擎：`createReactiveStateEngine` 只喂 `candleConfirmStreamTable`（`confirm=1`），
  输出 ma5/ma10/ma20/ema12/ema26/macd/mstd20/volMa5/high20/low20；
- tick 活跃度引擎：`createTimeSeriesEngine` 5 分钟窗口/1 分钟步长，输出
  count5m/open5m/high5m/low5m/close5m/avgPx/vol5m（供 `Kernel.getTradeInstid()` 排序）；
- 历史归档（P1）：`tradeflow/stream/archive.py` 将 tick/trade 落 `dfs://market`
  （`date(ts)` + `hash(instId)` 复合分区），供回测使用。启用方式（幂等）：

```python
from tradeflow.stream.archive import ensure_archive

ensure_archive()
```

## 订阅健康检查

```python
import json
print(json.dumps(kernel.health(), indent=2, ensure_ascii=False))
```

返回公共/业务/私有连接的 `connected / last_message_at / reconnect_count / error_count`
以及写线程 `queue / written / dropped / errors`。

## 配置

- `.env` 查找顺序：`TRADEFLOW_ENV_FILE` 指定路径 → 当前工作目录 → 包目录 → 源码仓库根目录；
- 真实环境变量优先级最高，`.env` 不会覆盖已存在的环境变量；
- 配置模板见 `.env.example`，复制为 `.env` 后填写；
- 日志默认写入调用脚本同目录的 `log/` 下，文件名为脚本名（如 `test.py` → `log/test.log`）；可用 `TRADEFLOW_LOG_DIR` 覆盖日志根目录；
- WebSocket 行为可用环境变量覆盖：`WS_RECV_TIMEOUT`（30s）、`WS_HEARTBEAT_INTERVAL`（20s）、
  `WS_RECONNECT_MAX`（30s）、`WS_QUEUE_SIZE`（200000）、`WS_BATCH_SIZE`（200）、`WS_FLUSH_INTERVAL`（0.2s）。

## 测试

```bash
# 单元测试（无需网络与 DolphinDB）
pytest -q

# DolphinDB 集成测试（会 drop/recreate 全部流表，
# 必须指向独立测试实例，不要对正在运行策略的库执行）
TRADEFLOW_TEST_DDB=1 TRADEFLOW_TEST_DDB_ALLOW_DROP=1 pytest tests/integration/test_stream_schema.py -q
TRADEFLOW_TEST_DDB=1 pytest tests/integration/test_archive.py -q
```

## 从 3.3.x 升级

- 表结构变更（时间列统一 TIMESTAMP、`fundingrateStreamTable` 等追加列、新增 14 张表）：
  首次运行请用 `Kernel(recreate_tables=True)` 重建流表；
  流表数据为实时总线数据（保留 1~6 小时），重建不影响 `dfs://` 历史库；
- 时间语义统一为 UTC-naive：与旧版本写入的本地时间数据混用的窗口内，
  指标可能有短暂偏差，重建流表即可消除；
- 断线重连后自动进行 REST 对账（订单/持仓/账户）与 K 线缺口补齐，无需人工干预。

## 版本记录

## version 3.4.0

- `subscribe.py` 重构为独立 `subscribe` 模块：`base`（连接/心跳/指数退避重连/动态订阅）、
  `channels`（频道清单唯一来源）、`handlers`（报文解析纯函数）、`manager`（多连接编排）、
  `writer`（独立线程批量写库）、`reconcile`（断线 REST 对账 + K 线缺口补齐）、`orderbook`（增量盘口序号校验）；
- 默认公共频道扩展为 tickers/trades/funding-rate/mark-price/index-tickers/
  open-interest/liquidation-orders/status/instruments；K 线默认 1m/5m；
- 新增 `Kernel` 参数 `market/depth/options/algo/recreate_tables` 与 `kernel.health()` 健康指标；
- DolphinDB 流表结构统一由 `stream/schema.py` 管理（34 张流表/快照表），时间列统一 UTC-naive TIMESTAMP；
- 新增流引擎：`candleConfirmStreamTable`（只喂 confirm=1）+ 响应式状态引擎（MA/MACD/波动）、
  时间序列引擎（tick 5 分钟活跃度）、7 个最新快照动作；
- 新增 `stream/archive.py` 将 tick/trade 归档到 `dfs://market`（date+hash 复合分区，实测验证）；
- `RiskManage` 改用 `getPositions()` 并以 `_closing` 集合抑制重复平仓单；
- 依赖升级：pandas>=2.2、websockets>=13、SQLAlchemy>=2.0。

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
