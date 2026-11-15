# strategy

量化策略开发目录，不随 `tradeflow` 包发布。

- `example.py`：最小策略模板，复制后修改即可；
- 策略类继承 `tradeflow.strategy.Strategy`，通过 `instIds` 声明交易对；
- 在项目根目录运行：`python -m strategy.example`。
