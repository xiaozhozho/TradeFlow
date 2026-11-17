# okx 模块来源说明

本目录是 [okxapi/python-okx](https://github.com/okxapi/python-okx) 的 **vendored（内嵌）副本**，
不是本项目自行维护的 SDK。

| 项目 | 值 |
| --- | --- |
| 上游仓库 | https://github.com/okxapi/python-okx |
| 上游版本 | 0.4.4 |
| 同步 commit | `06bb8a9916c0dd133902f57c923abc3c6605c0fe`（2026-09-07） |
| 同步日期 | 2026-11-15 |

## 目录内容

- `Account.py`、`Trade.py`、`MarketData.py` 等：**与上游保持一致，请勿直接修改**，
  便于后续整体覆盖升级。当前本地补丁（均为行为不变、消除废弃告警，升级覆盖后需重放）：
  1. `Affiliate.py`：`from okx.xxx` 绝对导入改为相对导入，否则作为
     `TradeFlow.okx` 子包使用时无法加载（上游以顶层包 `okx` 发布）；
  2. `utils.py` 的 `get_timestamp()`：`datetime.utcnow()` 改为带时区 `now()` 后去掉
     tzinfo（Python 3.12 起 utcnow 废弃），输出格式不变；
  3. `okxclient.py` 的 `_request()`：POST 请求体由 `data=` 改为 `content=`
     （httpx 废弃 `data=<str/bytes>`），线上字节与请求头不变。
- `api.py`：**本项目新增的适配层**，上游没有。用于把拆分的 API 类聚合为
  `Api(flag=...)`，供 `kernel.py` 使用；其中显式向各 API 传
  `use_server_time=None`，避免 `CopyTradingAPI` 的 `False` 默认值触发废弃告警。

## 升级方式

```bash
git clone --depth 1 https://github.com/okxapi/python-okx.git /tmp/python-okx
rm -f okx/*.py                       # 保留 okx/api.py
cp /tmp/python-okx/okx/*.py okx/     # 覆盖上游文件
```

覆盖后检查：

1. `okx/api.py` 引用的 API 类是否仍存在、类名是否变化；
2. 上游是否新增了必需依赖（当前为 `httpx[http2]`、`loguru`）；
3. 运行公共接口冒烟测试（见项目 README / 提交记录）。
