import logging
import os
from logging import FileHandler, StreamHandler
from datetime import datetime
from pathlib import Path


def _resolve_log_dir() -> Path:
    """日志目录：TRADEFLOW_LOG_DIR > 源码仓库根目录/log > 当前工作目录/log。"""
    override = os.getenv("TRADEFLOW_LOG_DIR")
    if override:
        return Path(override).expanduser()
    repo_root = Path(__file__).resolve().parents[2]
    if (repo_root / "pyproject.toml").is_file() or (repo_root / ".git").exists():
        return repo_root / "log"
    return Path.cwd() / "log"


LOG_DIR = _resolve_log_dir()


def get_logger():
    today = datetime.now().strftime("%Y-%m-%d")
    # 日志固定写入项目根目录的 log/，不依赖启动时的工作目录
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    filename = str(LOG_DIR / f'{today}.log')
    # FileHandler和StreamHandler分别对应将日志输出到文档、控制台
    logger = logging.getLogger(filename)  # 创建logger对象
    if logger.handlers:  # 已初始化过，避免重复添加 handler
        return logger
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

    return logger

