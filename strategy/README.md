# strategy

量化策略开发目录，不随 `tradeflow` 包发布。

## example.py

演示完整行情落库链路：

1. `Kernel.prepare()` 创建 DolphinDB 流数据表、共享内存表与 `lastTickAction` 流数据引擎；
2. `Kernel.subscribe()` 启动 OKX WebSocket 公共频道订阅，tick 行情写入 `tickStreamTable`（示例使用 `private=False`，无需私有 API Key）；
3. 策略每 3 秒统计一次 `tickStreamTable` 行数并输出。

运行（项目根目录）：

```bash
python -m strategy.example
```

日志默认写入 `strategy/log/example.log`，Ctrl+C 停止。

## ma_cross.py

MA5/MA15 均线交叉模拟盘策略（多合约，1 分钟 K 线）：

- 同时交易多个 U 本位永续（`INST_IDS` 默认 10 个：
  `BTC / ETH / SOL / DOGE / XRP / OKB / LTC / PEPE / XAU / BZ-USDT-SWAP`），
  每个合约独立判断信号、独立下单，互不影响（单合约失败不阻塞其他合约）；
- 金叉（MA5 上穿 MA15）做多；死叉（下穿）平多并反向做空（净持仓模式，
  `Kernel.init()` 已设置 net_mode）；
- 均线为声明式指标：`indicators = {"ma": [5, 15]}`，框架在
  `candleConfirmStreamTable` 上用 DolphinDB 响应式引擎自动计算
  （`indicator_MaCrossStrategyStreamTable`），策略不做 pandas 计算；
- 每个合约单笔保证金固定 10 USDT（按持仓合约数累加，10 个合约全部持仓
  约占用 100 USDT）：`buyByMargin/sellByMargin` 按 保证金 x 杠杆 /
  (价格 x 合约面值 ctVal) 自动换算张数，并由 Trader 规整与校验限额；
- 同一合约同一根 K 线上的交叉只执行一次（按合约记录信号 bar 的 ts）；
- 每轮 scan 先输出 `tickStreamTable` 当前数据量（观察行情落库是否正常），
  查询失败只记日志，不影响交易逻辑；
- 执行失败（保证金不足、限频、限额等）只记录日志，等待下一个交叉信号。

运行（项目根目录，`.env` 需配置模拟盘密钥 `OKX_API_KEY_SIMULATE` 等）：

```bash
python -m strategy.ma_cross
```

日志写入 `strategy/log/ma_cross.log`，Ctrl+C 停止。策略逻辑有离线单测
（`tests/test_ma_cross.py`，FakeDolphin + FakeApi，不连库不联网）。
