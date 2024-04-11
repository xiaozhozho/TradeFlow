import os
import sys
import logging
from logging import FileHandler, StreamHandler



if not os.path.exists('log') and sys.platform=='win32':
    os.makedirs('log')
    file = os.path.basename(sys.argv[0]).split('.')[0]
    filename = f'./log/{file}.log'

else:
    file = os.path.basename(sys.argv[0]).split('.')[0]
    filename = file + '.log'

# FileHandler和StreamHandler分别对应将日志输出到文档、控制台
logger = logging.getLogger(filename)  # 创建logger对象
logger.setLevel(logging.DEBUG)  # 配置Logger对象的日志级别
logfile = FileHandler(filename)  # 创建handler对象（输出到文件）
console = StreamHandler()  # 创建handler对象（输出到控制台）

logfile.setLevel(logging.DEBUG)  # 配置输出日志的级别
console.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s %(filename)s %(lineno)s %(message)s')  # 配置日志的输出格式
logfile.setFormatter(fmt=formatter)
console.setFormatter(fmt=formatter)

logger.addHandler(logfile)  # 添加处理程序的对象
logger.addHandler(console)



