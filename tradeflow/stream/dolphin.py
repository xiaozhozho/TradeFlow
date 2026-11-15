import threading
import time
import dolphindb as ddb
from pandas import DataFrame
from functools import lru_cache
import dolphindb.settings as keys
from .script import *
from ..setting import DOLPHINDB_HOST,DOLPHINDB_PORT,DOLPHINDB_USER,DOLPHINDB_PASSWORD

class Dolphin:
    """DolphinDB 数据库管理类,为了避免多个session的切换,考虑使用单例模式"""
    def __new__(cls, *args, **kargs):
        if not hasattr(cls, '__instance__'):
            cls.__instance__ = super().__new__(cls)
        return cls.__instance__

    def __init__(self, host=DOLPHINDB_HOST, port=DOLPHINDB_PORT, userid=DOLPHINDB_USER, password=DOLPHINDB_PASSWORD) -> None:
        # 单例守卫：重复 Dolphin() 不重建 session（否则会打断正在使用的连接）
        if getattr(self, '_initialized', False):
            return
        self._initialized = True
        self.host = host 
        self.port = port 
        self.userid = userid
        self.password = password
        # 多线程（StreamWriter 写线程 / 主线程）共用实例，所有脚本执行加锁
        self._lock = threading.RLock()
        # 注1：新版客户端的 keepAliveTime 单位为秒且有上限，不能传 TIMEOUT(999999)，
        #      使用客户端默认的网络超时即可；TIMEOUT 仅用于 WebSocket 订阅。
        # 注2：凭据必须在 connect() 时传入，只传给构造函数无效，
        #      否则会话会以 guest(匿名) 身份登录，建库等操作会报 NotAuthenticated。
        self.session = ddb.session(enableASYNC=False)
        self.session.connect(host=host, port=port, userid=userid, password=password, reconnect=True)
        self.asyncSession = ddb.session(enableASYNC=True)
        self.asyncSession.connect(host=host, port=port, userid=userid, password=password, reconnect=True)

    def newSession(self, enableAsync: bool = False):
        """创建一条独立连接（用于 StreamWriter 等独立线程，避免 session 串扰）。"""
        session = ddb.session(enableASYNC=enableAsync)
        session.connect(
            host=self.host, port=self.port,
            userid=self.userid, password=self.password,
            reconnect=True,
        )
        return session

    @lru_cache()
    def getSession(self, enableAsync=False, isCompress=False, highAvailability=False):
        """ 获取session  """
        session = ddb.session(enableASYNC=enableAsync, compress=isCompress, highAvailability=highAvailability, highAvailabilitySites=['localhost:8848'])
        session.connect(self.host, self.port, self.userid, self.password)
        return session

    def upload(self, var, aliasName):
        """ 上传本地变量到服务端 """
        self.session.upload({aliasName: var})

    def close(self):
        """ 关闭session """
        self.session.close()

    def clearAllCache(self):
        """ 清除服务端缓存 """
        script="clearAllCache()"
        self.runScript(script, enableAsync=True)
        print('All caches have been cleaned up!')

    def loadText(self, filePath, delimiter=','):
        """ 读取文件内容 """
        data = self.session.loadText(filePath, delimiter=delimiter)
        df = data.toDF()
        return df

    def ploadText(self, filePath, delimiter=','):
        """" 异步读取文件内容 """
        data = self.session.ploadText(filePath, delimiter=delimiter)
        df = data.toDF()
        return df

    def loadTable(self, dbPath, tableName):
        """ 加载数据库表 """
        table = self.session.loadTable(tableName=tableName, dbPath=dbPath)
        return table

    def df2Table(self, dbPath, tableName, df, enableAsync=False):
        """将df采用异步或非异步的方式写入数据库"""
        script = f"append!{{loadTable('{dbPath}', `{tableName})}}"
        self.runScript(script, df, enableAsync=enableAsync)

    def df2TableAppender(self, df, dbPath, tableName, partitionColName, connectNum=20):
        """ 并发将data写入数据库 """
        pool = ddb.DBConnectionPool(self.host, self.port, connectNum, self.user, self.password)
        appender = ddb.PartitionedTableAppender(dbPath, tableName, partitionColName, pool)
        result = appender.append(df)
        return result

    def runScript(self, *script, enableAsync=False, clearMemory=False):
        """ 运行脚本文件（加锁，避免多线程共用 session 串扰） """
        with self._lock:
            if enableAsync:
                self.asyncSession.run(*script, clearMemory=clearMemory)
            else:
                result = self.session.run(*script, clearMemory=clearMemory)
                return result

    def prepare_streams(self, table_names, recreate=False):
        """按 schema 注册表幂等建表；recreate=True 时先删后建（会丢数据）。

        所有表合并在单次 runScript 中执行（减少网络往返，避免逐表调用时
        服务器瞬时无响应导致客户端无限阻塞），执行后统一校验缺表并报错。
        """
        from .schema import STREAM_TABLES
        names = list(table_names)
        unknown = [name for name in names if name not in STREAM_TABLES]
        if unknown:
            raise KeyError(f'未注册的流表: {unknown}')
        blocks = []
        for name in names:
            spec = STREAM_TABLES[name]
            body = spec.ensure_script(recreate=recreate)
            blocks.append(
                f"try {{ {body} }} catch(ex) {{ print(\"prepare {name} failed: \" + ex) }}"
            )
        checks = "\n".join(
            f'if (!{STREAM_TABLES[name].exists_script()}) {{ missing = missing + "{name}" }}'
            for name in names
        )
        script = f"""
missing = string([])
{chr(10).join(blocks)}
{checks}
missing
"""
        missing = self.runScript(script) or []
        if missing:
            raise RuntimeError(f'流表创建失败: {list(missing)}')
        print(f"prepared {len(names)} stream tables!")

    def prepare_engines(self, engine_names):
        """按 engines 注册表重建流引擎与订阅。"""
        from .engines import ENGINE_SPECS, create_engine_script
        for name in engine_names:
            self.runScript(create_engine_script(ENGINE_SPECS[name]))
            print(f"prepared engine {name}!")

    def data2MemoryTable(self, tableName, *data, enableAsync=False, clearMemory=False):
        """ 将 data: DataFrame or iterable 插入内存表中 """
        script = "tableInsert{%s}" % tableName
        if len(data)>0:
            self.runScript(script, *data, enableAsync=enableAsync, clearMemory=clearMemory)

    def getSchema(self, dbPath, table):
        """ 获取分区数据库表结构 """
        script=f"""
        pt=loadTable('{dbPath}',`{table})
        schema(pt);
        """
        result = self.runScript(script)
        return result

    def selectBySQL(self, dbPath, tableName, sql, clearMemory=False):
        """ 使用SQL语句查询分区数据库内容 """
        script = f"""
        db=database("{dbPath}")
        pt=loadTable(db,`{tableName})
        {sql};
        """
        result = self.runScript(script, clearMemory=clearMemory)
        return result

    def dropDatabase(self, dbPath, force=False):
        """ 删除分区数据库 """
        warningDropDB = ['',]
        if dbPath in warningDropDB and force==False:
            raise ValueError(f'This database of {dbPath} needs to be stored for a long time. Please delete it carefully!')
        if self.session.existsDatabase(dbPath):
            self.session.dropDatabase(dbPath)
            print(f"{dbPath}: droped"+" successfully!")

    def getAllDB(self):
        """ 获取所有数据库情况 """
        script = """getAllDBs()"""
        allDB = self.runScript(script, clearMemory=True)
        return allDB
    
    def createDatabase(self, dbName, partitionType, partitions, dbPath, existDrop=True):
        """" 创建分区数据库 """
        if existDrop:
            self.dropDatabase(dbPath)

        if partitionType == 'value':
            db = self.session.database(dbName=dbName, dbPath=dbPath, partitionType=keys.VALUE, partitions=partitions)
        elif partitionType == 'range':
            db = self.session.database(dbName=dbName, dbPath=dbPath, partitionType=keys.RANGE, partitions=partitions)
        elif partitionType == 'list':
            db = self.session.database( dbName=dbName, dbPath=dbPath,partitionType=keys.LIST, partitions=partitions)
        elif partitionType == 'hash':
            db = self.session.database(dbName=dbName, dbPath=dbPath, partitionType=keys.HASH, partitions=partitions)
        elif partitionType == 'compo':
            db = self.session.database(dbName=dbName, dbPath=dbPath, partitionType=keys.COMPO, partitions=partitions) 
        else:
            db = self.session.database(dbName=dbName, dbPath=dbPath)
        print(f"{dbPath} created successfully!")
        return db

    def createPartitionedTable(self, db, tableName, partitionColumns, data):
        """ 创建分区表 """
        t = self.session.table(data=data)
        db.createPartitionedTable(table=t, tableName=tableName, partitionColumns=partitionColumns).append(t)
        print(f"created {tableName}!")

    def unsubscribeTable(self, tableName, actionName):
        """取消所有流数据表的订阅"""
        script = """
        try{unsubscribeTable('','%s','%s')}catch(ex){print('ubsubscribe %s on %s')}
        """ % (tableName,actionName, tableName, actionName)
        self.runScript(script)

    def dropStreamTable(self, tableName):
        """ 删除流数据表 """
        script = "try{dropStreamTable('%s')}catch(ex){print('droped %s falled!')}" % (tableName,tableName)
        self.runScript(script)
            
    def dropStreamEngine(self, engineName):
        """ 删除流数据表对应引擎"""
        script = "try{dropStreamEngine('%s')}catch(ex){print('droped %s failed!')}" % (engineName, engineName)
        self.runScript(script)

    # ***********   关于流数据表、内存表的函数     **************

    # ***********   流数据表创建：统一委托 schema 注册表   **************
    # 旧方法保留原签名（kernel.py 等调用方无需改动），
    # 建表逻辑统一走 prepare_streams：幂等建表，existDrop=True 时先删后建。
    def prepare(self, table_names, recreate=False):
        """批量建表入口。"""
        self.prepare_streams(table_names, recreate=recreate)

    def createProductStreamTable(self, existDrop=True):
        self.prepare_streams(["productStreamTable"], recreate=existDrop)

    def createTickStreamTable(self, existDrop=True):
        self.prepare_streams(["tickStreamTable"], recreate=existDrop)

    def createShareTickTable(self):
        self.prepare_streams(["shareTickTable"])

    def createCandleStreamTable(self, existDrop=True):
        self.prepare_streams(["candleStreamTable"], recreate=existDrop)

    def createOpenInterestStreamTable(self, existDrop=True):
        self.prepare_streams(["openinterestStreamTable"], recreate=existDrop)

    def createEstimatedPriceStreamTable(self, existDrop=True):
        self.prepare_streams(["estimatedpriceStreamTable"], recreate=existDrop)

    def createMarkPriceStreamTable(self, existDrop=True):
        self.prepare_streams(["markpriceStreamTable"], recreate=existDrop)

    def createMarkPriceCandleStreamTable(self, existDrop=True):
        self.prepare_streams(["markpricecandleStreamTable"], recreate=existDrop)

    def createPriceLimitStreamTable(self, existDrop=True):
        self.prepare_streams(["pricelimitStreamTable"], recreate=existDrop)

    def createOptSummaryStreamTable(self, existDrop=True):
        self.prepare_streams(["optsummaryStreamTable"], recreate=existDrop)

    def createFundingRateStreamTable(self, existDrop=True):
        self.prepare_streams(["fundingrateStreamTable"], recreate=existDrop)

    def createIndexCandleStreamTable(self, existDrop=True):
        self.prepare_streams(["indexcandleStreamTable"], recreate=existDrop)

    def createIndexTickStreamTable(self, existDrop=True):
        self.prepare_streams(["indextickStreamTable"], recreate=existDrop)

    def createOptionTradeStreamTable(self, existDrop=True):
        self.prepare_streams(["optiontradeStreamTable"], recreate=existDrop)

    def createLiquidationOrderStreamTable(self, existDrop=True):
        self.prepare_streams(["liquidationorderStreamTable"], recreate=existDrop)

    def createStatusStreamTable(self, existDrop=True):
        self.prepare_streams(["statusStreamTable"], recreate=existDrop)

    # ***********  私有频道-流数据表创建  *****************
    def createShareAccountTable(self):
        self.prepare_streams(["shareAccountTable"])

    def createSharePositionTable(self):
        self.prepare_streams(["sharePositionTable"])

    def createShareBalanceTable(self):
        self.prepare_streams(["shareBalanceTable"])

    def createOrderStreamTable(self, existDrop=True):
        self.prepare_streams(["orderStreamTable"], recreate=existDrop)

    def createShareOrderTable(self):
        self.prepare_streams(["shareOrderTable"])

    def createShareWarningTable(self):
        self.prepare_streams(["shareWarningTable"])
