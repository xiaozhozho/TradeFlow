import time
import asyncio
import pandas as pd
from functools import wraps
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed
from joblib import Parallel, delayed
import psutil
cpuNumber = psutil.cpu_count(logical=True)
sessionLock = asyncio.Lock()


def timeit(runNum=1):
    """耗时测试装饰器"""
    def middle(function):
        @wraps(function)
        def wrap(*args, **kwargs):
            start = time.time()
            if runNum != 1:
                for i in range(runNum):
                    result = function(*args, **kwargs)
                    if i == runNum-1:
                        totalConsumption = round(time.time()-start, 2) 
                        perConsumption = round((time.time()-start) / runNum, 2) 
                        print(f'The function {function.__name__} runs in a cycle for {runNum} times, taking {totalConsumption} seconds in total, and {perConsumption} seconds in average!')
                        return result
            else:
                result = function(*args, **kwargs)
                consumption = round(time.time()-start, 2) 
                print(f'The function {function.__name__} consumes: {consumption} seconds!')
                return result
        return wrap
    return middle


def runThreads(threadNum):
    """单线程改装为多线程装饰器"""
    @wraps(runThreads)
    def middle(function):
        @wraps(function)
        def wrap(params):
            results = []
            print(f'The {function.__module__}.{function.__name__} to start {threadNum} threads to speed up.')
            executor = ThreadPoolExecutor(threadNum)
            tasks = [executor.submit(function, *param) for param in params]
            for task in as_completed(tasks):
                result = task.result()
                results.append(result)
            return results
        return wrap
    return middle

def runProcesses(processNum=cpuNumber-2):
    """单线程改装为多线程装饰器"""
    @wraps(runProcesses)
    def middle(function):
        @wraps(function)
        def wrap(args, **kwargs):
            print(f'The {function.__module__}.{function.__name__} to start {processNum} processes to speed up.')
            results = Parallel(n_jobs=processNum)(delayed(function)(arg, **kwargs) for arg in args) 
            return results
        return wrap
    return middle

def runAsync(function):
    """正常调用改异步调用装饰器"""
    @wraps(function)
    def wrap(params):
        loop = asyncio.get_event_loop()
        coros = [loop.create_task(function(*param)) for param in params]
        results = loop.run_until_complete(asyncio.gather(*coros))
        loop.close()
        return results
    return wrap

def lock(function):
    """使用session时给session加锁"""
    @wraps(function)
    async def warp(self, *args, **kwargs):
        async with sessionLock:
            result = await function(self, *args, **kwargs)
            return result
    return warp

def runLoop(function):
    """事件循环调用装饰器"""
    @wraps(function)
    def wrap(*args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(function(*args, **kwargs))
    return wrap

def runClassLoop(function):
    """事件循环调用装饰器"""
    @wraps(function)
    def wrap(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(function(self, *args, **kwargs))
    return wrap

def runGroups(processNum=cpuNumber/2):
    """调用parallel并行计算每个group,默认使用PC一半的逻辑核心数"""
    @wraps(runGroups)
    def middle(function):
        @wraps(function)
        def wrap(groups, **kwargs):
            'Function to start 10 processes to speed up'
            print(f'The {function.__module__}.{function.__name__} to start {processNum} processes to speed up.')
            group_list = Parallel(n_jobs=processNum)(delayed(function)(group, **kwargs) for name, group in groups) 
            result = pd.concat(group_list, axis=0)
            return result
        return wrap
    return middle

def runClassGroups(processNum=cpuNumber/2):
    """调用parallel并行计算每个group,默认使用PC一半的逻辑核心数"""
    @wraps(runClassGroups)
    def middle(function):
        @wraps(function)
        def wrap(self, groups, **kwargs):
            print(f'The {function.__module__}.{self.__class__.__name__}.{function.__name__} to start {processNum} processes to speed up.')
            group_list = Parallel(n_jobs=processNum)(delayed(function)(self, group, **kwargs) for name, group in groups) 
            result = pd.concat(group_list, axis=0)
            return result
        return wrap
    return middle


