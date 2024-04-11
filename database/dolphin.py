import time
import dolphindb as ddb
from pandas import DataFrame
from functools import lru_cache
import dolphindb.settings as keys
from ..utils.log import logger
from ..utils.decorator import timeit   
from .script import * 
from ..setting import DOLPHINDB_HOST,DOLPHINDB_PORT,DOLPHINDB_USER,DOLPHINDB_PASSWORD,TIMEOUT

class Dolphin:
    """DolphinDB 数据库管理类,为了避免多个session的切换,考虑使用单例模式"""
    def __new__(cls, *args, **kargs):
        if not hasattr(cls, '__instance__'):
            cls.__instance__ = super().__new__(cls, *args, **kargs)
        return cls.__instance__

    def __init__(self, host=DOLPHINDB_HOST, port=DOLPHINDB_PORT, userid=DOLPHINDB_USER, password=DOLPHINDB_PASSWORD) -> None:
        self.host = host 
        self.port = port 
        self.userid = userid
        self.password = password
        self.session = ddb.session(userid=userid, password=password, enableASYNC=False)
        self.session.connect(host=host, port=port, keepAliveTime=TIMEOUT, reconnect=True)
        self.asyncSession = ddb.session(userid=userid, password=password, enableASYNC=True)
        self.asyncSession.connect(host=host, port=port, keepAliveTime=TIMEOUT, reconnect=True)

    @lru_cache()
    def getSession(self, enableAsync=False, isCompress=False, highAvailability=False):
        """ 获取session  """
        session = ddb.session(enableASYNC=enableAsync, compress=isCompress, highAvailability=highAvailability, highAvailabilitySites=['localhost:8848'])
        session.connect("localhost", 8848, "admin", "***REMOVED***")
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
        """ 运行脚本文件 """
        if enableAsync:
            self.asyncSession.run(*script, clearMemory=clearMemory)
        else:
            result = self.session.run(*script, clearMemory=clearMemory)
            return result

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
    # (1) 公共频道-流数据表创建
    def createStreamTable(self, tableName, colName, colType, cacheSize=500000, retentionMinutes=300, preCache=500000):
        """ 创建普通流数据表 """
        script = f"""
        colName = {colName}
        colType = {colType}
        {tableName} = streamTable({cacheSize}:0, colName, colType)
        enableTableShareAndPersistence({tableName}, "{tableName}StreamTable", false, true, cacheSize={cacheSize}, retentionMinutes={retentionMinutes}, preCache={preCache});
        """
        try:
            self.runScript(script)
            print(f'created {tableName}!')
        except Exception as e:
            print(f'createe {tableName} failed for {e}!')

    def createProductStreamTable(self, existDrop=True):
        """ 创建产品 流数据表 """
        try:
            self.runScript(createProductStreamTableScript)
            print('created productStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('productStreamTable')
                self.runScript(createProductStreamTableScript)
                print('created productStreamTable!')
            else:
                raise e

    def createTickStreamTable(self, existDrop=True):
        """ 创建tick流数据表 """
        try:
            self.runScript(createTickStreamTableScript)
            print('created tickStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('tickStreamTable')
                self.runScript(createTickStreamTableScript)
                print('created tickStreamTable!')
            else:
                pass

    def createShareTickTable(self):
        """ 创建最新tick共享内存表 """
        self.runScript(createShareTickTableScript)
        print('created shareTickTable!')

    def createLastTickEngine(self):
        """创建最新tick内存表的订阅引擎"""
        script = """
        try
            {subscribeTable(tableName=`tickStreamTable, actionName="lastTickAction", offset=0, handler=tableInsert{shareTickTable}, msgAsTable=true)}
        catch(ex)
            {
                unsubscribeTable(tableName='tickStreamTable', actionName='lastTickAction')
                subscribeTable(tableName=`tickStreamTable, actionName="lastTickAction", offset=0, handler=tableInsert{shareTickTable}, msgAsTable=true)
            }
        """
        self.runScript(script)

    def createCandleStreamTable(self, existDrop=True):
        """创建candle流数据表"""
        try:
            self.runScript(createCandleStreamTableScript)
            print('created candleStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('candleStreamTable')
                self.runScript(createCandleStreamTableScript)
                print('created candleStreamTable!')
            else:
                pass

    def createOpenInterestStreamTable(self, existDrop=True):
        """" 创建持仓流数据表 """
        try:
            self.runScript(createOpenInterestStreamTableScript)
            print('create openinterestStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('openinterestStreamTable')
                self.runScript(createOpenInterestStreamTableScript)
                print('create openinterestStreamTable!')
            else:
                pass

    def createEstimatedPriceStreamTable(self, existDrop=True):
        """ 创建交割/行权价格 流数据表 """
        try:
            self.runScript(createEstimatedPriceStreamTableScript)
            print('create estimatedpriceStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('estimatedpriceStreamTable')
                self.runScript(createEstimatedPriceStreamTableScript)
                print('created estimatedpriceStreamTable!')
            else:
                pass

    def createMarkPriceStreamTable(self, existDrop=True):
        """ 创建标记价格 流数据表 """
        try:
            self.runScript(createMarkPriceStreamTableScript)
            print('created markpriceStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('markpriceStreamTable')
                self.runScript(createMarkPriceStreamTableScript)
                print('created markpriceStreamTable!')
            else:
                pass

    def createMarkPriceCandleStreamTable(self, existDrop=True):
        """ 创建标记价格K线 流数据表 """
        try:
            self.runScript(createMarkPriceCandleStreamTableScript)
            self.print('created markpricecandleStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('markpricecandleStreamTable')
                self.runScript(createMarkPriceCandleStreamTableScript)
                print('created markpricecandleStreamTable!')
            else:
                pass

    def createPriceLimitStreamTable(self, existDrop=True):
        """ 创建限价 流数据表 """
        try:
            self.runScript(createPriceLimitStreamTableScript)
            print('created pricelimitStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('pricelimitStreamTable')
                self.runScript(createPriceLimitStreamTableScript)
                print('created pricelimitStreamTable!')
            else:
                pass

    def createOptSummaryStreamTable(self, existDrop=True):
        """ 创建期权汇总 流数据表 """
        try:
            self.runScript(createOptSummaryStreamTableScript)
            print('created optsummaryStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('optsummaryStreamTable')
                self.runScript(createOptSummaryStreamTableScript)
                print('created optsummaryStreamTable!')
            else:
                pass

    def createFundingRateStreamTable(self, existDrop=True):
        """ 创建资金费率 流数据表 """
        try:
            self.runScript(createFundingRateStreamTableScript)
            print('created fundingrateStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('fundingrateStreamTable')
                self.runScript(createFundingRateStreamTableScript)
                print('created fundingrateStreamTable!')
            else:
                pass

    def createIndexCandleStreamTable(self, existDrop=True):
        """ 创建指数数据 流数据表 """
        try:
            self.runScript(createIndexCandleStreamTableScript)
            print('created indexcandleStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('indexcandleStreamTable')
                self.runScript(createIndexCandleStreamTableScript)
                print('created indexcandleStreamTable!')
            else:
                pass

    def createIndexTickStreamTable(self, existDrop=True):
        """ 创建指数tick 流数据表 """
        try:
            self.runScript(createIndexTickStreamTableScript)
            print('created indextickStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('indextickStreamTable')
                self.runScript(createIndexTickStreamTableScript)
                print('created indextickStreamTable!')
            else:
                pass

    def createOptionTradeStreamTable(self, existDrop=True):
        """ 创建期权交易 流数据表"""
        try:
            self.runScript(createOptionTradeStreamTableScript)
            print('created optiontradeStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('optiontradeStreamTable')
                self.runScript(createOptionTradeStreamTableScript)
                print('created optiontradeStreamTable!')
            else:
                pass

    def createLiquidationOrderStreamTable(self, existDrop=True):
        """ 创建公共频道爆仓信息 流数据表 """
        try:
            self.runScript(createLiquidationOrderStreamTableScript)
            print('created liquidationorderStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('liquidationorderStreamTable')
                self.runScript(createLiquidationOrderStreamTableScript)
                print('created liquidationorderStreamTable!')
            else:
                pass

    def createStatusStreamTable(self, existDrop=True):
        """ 创建系统维护状态 流数据表 """
        try:
            self.runScript(createStatusStreamTableScript)
            print('created statusStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('statusStreamTable')
                self.runScript(createStatusStreamTableScript)
                print('created statusStreamTable!')
            else:
                pass


    # ***********  私有频道-流数据表创建  *****************
    def createShareAccountTable(self):
        """ 创建账户信息 内存数据表"""
        self.runScript(createShareAccountTableScript)
        print('created shareAccountTable!')
        
    def createSharePositionTable(self):
        """ 创建持仓最新索引内存表 """
        self.runScript(createSharePositionTableScript)
        print('created sharePositionTable!')

    def createShareBalanceTable(self):
        """ 创建余额 内存数据表 """
        self.runScript(createShareBalanceTableScript)
        print('created shareBalanceTable!')

    def createOrderStreamTable(self, existDrop=True):
        """ 创建订单 流数据表 """
        try:
            
            self.runScript(createOrderStreamTableScript)
            print('created orderStreamTable!')
        except RuntimeError as e:
            if existDrop:
                self.dropStreamTable('orderStreamTable')
                self.runScript(createOrderStreamTableScript)
                print('created orderStreamTable!')
            else:
                raise e

    def createShareWarningTable(self):
        """" 创建持仓预警内存表 """
        self.runScript(createShareWarningTableScript)
        print('created shareWarningTable!')











