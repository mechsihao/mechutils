# -*- coding:utf-8 -*-
import time
import sys
from tqdm import tqdm
import numpy as np
from functools import partial, wraps
import threading
import datetime

from concurrent.futures import ThreadPoolExecutor


def today(fmt="%Y%m%d.%H%M"):
    return datetime.datetime.today().strftime(fmt)


def timestamp2str(timestamp, fmt="%Y%m%d %H:%M:%S"):
    time_local = time.localtime(int(timestamp))
    return time.strftime(fmt, time_local)


def now(fmt="%Y.%m.%d-%H:%M:%S", with_week=False):
    r = today(fmt)
    if with_week:
        r += ', 星期' + ['一', '二', '三', '四', '五', '六', '日'][time.localtime().tm_wday]
    return  r


def threaded_function_wrapper(fun, executor, i, d):
    data = {"inputs": d, "thread_id": i}
    future = executor.submit(fun, data)
    future.index = i
    return future


def multi_threading_execution(fun, inputs, k):
    """
    fun: 执行的函数
    inputs: 传参
    k: 线程数
    """
    b = [None] * k
    with ThreadPoolExecutor(max_workers=k) as executor:
        futures = [threaded_function_wrapper(fun, executor, i, inputs[i]) for i in range(k)]
        for future in futures:
            b[future.index] = future.result()
    return b


def multi_threading_execution_for_mutlifuncs(funs, inputs):
    """
    fun: 执行的函数
    inputs: 传参
    k: 线程数
    """
    assert len(funs) == len(inputs), "[ERROR] funs nums must equal with inputs nums"
    b = [None] * len(funs)
    with ThreadPoolExecutor(max_workers=len(funs)) as executor:
        futures = [threaded_function_wrapper(funs[i], executor, i, inputs[i]) for i in range(len(funs))]
        for future in futures:
            b[future.index] = future.result()
    return b


def fun_wrapper(input_dict, fun, fun_type="one_sample", sleep_time=None):
    """
    fun_type: 为了支持多种类型的fun
        - one_sample: 每个fun输入为list中的一个元素，fun本身输入为一个元素
        - list_sample: 每个fun输入为list的子list，fun本身输入为一个list
    """
    inputs = input_dict["inputs"]
    thread_id = input_dict["thread_id"]
    if fun_type == "one_sample":
        start = time.time()
        res = []
        step = 1000
        for i, input_value in enumerate(inputs, 1):
            tmp_res = fun(input_value)
            res.append(tmp_res)
            if i % step == 0:
                cost_time = time.time() - start
                speed = step / cost_time
                total_time = len(inputs) / speed
                start = time.time()
                print("[THREAD INFO] <{}> Thread {} {}/{}, {}s/{}s".format(now(), thread_id, i * step, len(inputs), cost_time, total_time))
            if sleep_time:
                time.sleep(sleep_time)
    elif fun_type == "list_sample":
        res = fun(inputs)
    else:
        raise Exception("[ERROR] Unknown fun_type = '{}', expect ['one_sample', 'list_sample']".format(fun_type))
    return res


def split_list_uniformly(inputs, k):
    n = len(inputs)
    if k >= n:
        # 如果k大于等于列表长度，每份最多一个元素
        return [[item] for item in inputs] + [[] for _ in range(k - n)]
    
    # 计算基础大小和需要额外加1的份数
    base_size = n // k
    extra_items = n % k
    
    result = []
    start_idx = 0
    
    for i in range(k):
        # 前extra_items份多分配一个元素
        current_size = base_size + (1 if i < extra_items else 0)
        end_idx = start_idx + current_size
        
        chunk = inputs[start_idx:end_idx]
        result.append(chunk)
        
        start_idx = end_idx
    
    return result


def split_list_with_ids(inputs, k, num_len=3, with_thread_id=True):
    """带线程ID的均匀切分"""
    chunks = split_list_uniformly(inputs, k)
    package_inputs = []
    for i, chunk in enumerate(chunks):
        num_tag = "{}".format(i).zfill(num_len)
        if with_thread_id:
            package_inputs.append((num_tag, chunk))
        else:
            package_inputs.append(chunk)
    return package_inputs


def parall_fun(fun, inputs, k, fun_type="one_sample", sleep_time=None, qps_info=False, with_thread_id=False):
    """
    按照顺序多线程执行程序
        - fun: 要多线程执行的程序
        - inputs: 输入list，会被切分程k份输入到fun中多线程执行，多线程的函数必须是一个输入，因此需要你把一个输入数据打包成一个tuple
        - k: 线程数
        - fun_type: 为了支持多种类型的fun
            + one_sample: 每个fun输入为list中的一个元素，fun本身输入为一个元素
            + list_sample: 每个fun输入为list的子list，fun本身输入为一个list
        - with_thread_id: 每个函数输入会和分配的线程ID组成tuple传入，因此多线程处理的函数需要专门适配
    """
    num_len = len(str(k))
    all_num = len(inputs)
    start = time.time()
    package_inputs = split_list_with_ids(inputs, k, num_len=num_len, with_thread_id=with_thread_id)
    wrapped_fun = partial(fun_wrapper, fun=fun, fun_type=fun_type, sleep_time=sleep_time)
    m_res = multi_threading_execution(wrapped_fun, package_inputs, k)
    end = time.time()
    res = []
    for line in m_res:
        if isinstance(line, list):
            res.extend(line)
        else:
            res.append(line)
    
    if qps_info:
        print("\n"*k)
        print("[QPS INFO]")
        print("总数据量: %s"%all_num)
        print("总耗时: %s s"%(end-start))
        print("总并发数: %s"%k)
        print("总QPS: %s"%(all_num/(end-start)))
        print("高并发单线程QPS: %s"%(all_num/(end-start)/k))

    return res


def parall(k, fun_type, sleep_time=None):
    """并行化装饰器，要求被并行化的函数必须输入一个参数，如果有多个参数请用tuple或者dict在原始外面包一层，使其变为单参数list型输入，输出也为相同尺寸的list
    Arguments:
        k {int} -- [description]
    Returns:
        [type] -- [description]
    k: 
    fun_type: 原始函数运行的方式，有两种，分别代表原始函数是只处理一个元素还是处理多个元素组成的list
        - one_sample
        - list_sample
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            assert args == 1, "[ERROR] Bad params type num: {}".format(len(args))
            assert isinstance(args[0], list), "[ERROR] Bad params type: {}".format(type(args[0]))
            return parall_fun(func, args[0], k=k, fun_type=fun_type, sleep_time=sleep_time, qps_info=False)
        return wrapper
    return decorator


def parall_funs(funs, inputs):
    wrapped_funs = [partial(fun_wrapper, fun=fun, fun_type="list_sample") for fun in funs]
    m_res = multi_threading_execution_for_mutlifuncs(wrapped_funs, inputs)
    return m_res