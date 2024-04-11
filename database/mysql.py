import pymysql
import pandas as pd
from sqlalchemy import create_engine, text
from ..setting import MYSQL_HOST,MYSQL_PORT,MYSQL_USER,MYSQL_PASSWORD,MYSQL_DATABASE


class MySQL:
    """ MySQL 数据库管理类 """
    def __init__(self, host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE) -> None:
        self.host = host
        self.port = port
        self.user = user 
        self.password = password
        self.database = database
        # salalchemy
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
        self.conn = self.engine.connect()
        # pymysql
        self.connection = pymysql.connect(host=host, user=user, password=password, database=database)
        self.cursor = self.connection.cursor()

    def selectBySQL(self, sql):
        """使用SQL查询, 返回DataFrame表格"""
        data = pd.io.sql.read_sql(text(sql), con=self.conn)
        return data

    def insert2Table(self, data, tableName, index=False, create=False):
        """将DataFrame 插入数据库表"""
        if create:
            data.to_sql(tableName, con=self.engine, index=index)
        else:
            data.to_sql(tableName, con=self.engine, if_exists='append', index=index)

    def excuteSQL(self, sql):
        """单独执行SQL 语句, 主要用于个别数据的插入和更新等"""
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            raise ValueError(f'SQL Error:{sql}')
