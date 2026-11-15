"""日志工具：日志文件按调用脚本命名，并放在脚本同目录的 log/ 下。"""
import inspect
import logging
import os
from logging import FileHandler, StreamHandler
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
_IGNORED_MODULES = ("importlib", "runpy")


def _is_ignored_module(module: str) -> bool:
    return any(module == name or module.startswith(f"{name}.") for name in _IGNORED_MODULES)


def _caller_script() -> Path | None:
    """定位调用 get_logger 的用户脚本。

    跳过 tradeflow 包内部帧与导入机制，例如：
    - test.py 中直接调用 -> 返回 test.py；
    - Kernel 内部调用 -> 继续向上找到实例化 Kernel 的脚本。
    """
    frame = inspect.currentframe()
    try:
        frame = frame.f_back
        while frame is not None:
            module = frame.f_globals.get("__name__", "")
            filename = frame.f_globals.get("__file__")
            if filename and not str(filename).startswith("<") and not _is_ignored_module(module):
                path = Path(filename).resolve()
                if not path.is_relative_to(_PACKAGE_ROOT):
                    return path
            frame = frame.f_back
    finally:
        del frame
    return None


def _log_file() -> Path:
    """日志路径：TRADEFLOW_LOG_DIR（若设置）或 <脚本目录>/log/<脚本名>.log。"""
    script = _caller_script()
    override = os.getenv("TRADEFLOW_LOG_DIR")
    if override:
        directory = Path(override).expanduser()
    elif script is not None:
        directory = script.parent / "log"
    else:
        directory = Path.cwd() / "log"
    name = script.stem if script is not None else "main"
    return directory / f"{name}.log"


def get_logger():
    log_path = _log_file()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    filename = str(log_path)
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
