"""Batch 数据结构：handler 与写线程之间的传输单元。"""
from dataclasses import dataclass

import pandas as pd


@dataclass
class Batch:
    """一个待写入的批次：目标表 + 已解析 DataFrame。"""
    table: str
    frame: pd.DataFrame
