# TradeFlow



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