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
