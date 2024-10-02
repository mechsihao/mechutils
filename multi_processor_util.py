import numpy as np
from functools import partial
from concurrent.futures import ThreadPoolExecutor


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


def fun_wrapper(input_dict, fun, fun_type="one_sample"):
    """
    fun_type: 为了支持多种类型的fun
        - one_sample: 每个fun输入为list中的一个元素，fun本身输入为一个元素
        - list_sample: 每个fun输入为list的子list，fun本身输入为一个list
    """
    inputs = input_dict["inputs"]
    thread_id = input_dict["thread_id"]
    if fun_type == "one_sample":
        res = []
        for input_id, input_value in enumerate(inputs):
            print(f"[THREAD INFO] Thread {thread_id} {input_id}-th input={input_value}\n", end="")
            tmp_res = fun(input_value)
            res.append(tmp_res)
    elif fun_type == "list_sample":
        res = fun(inputs)
    else:
        raise Exception(f"[ERROR] Unknown fun_type = '{fun_type}', expect ['one_sample', 'list_sample']")
    return res


def parall_fun(fun, inputs, k, data_split="avg", fun_type="one_sample"):
    """
    按照顺序多线程执行程序
        - fun: 要多线程执行的程序
        - inputs: 输入list，会被切分程k份输入到fun中多线程执行
        - k: 线程数
        - data_split: 
            + 默认avg: 平均切分，多线程数据切分形式，否则需要用冒号分割，比如：1:1:2
            + 按照比例切分: 需要用冒号分割，比如：1:1:2
            + raw: 直接输入切好的数据，输入数量需要和k一致
        - fun_type: 为了支持多种类型的fun
            + one_sample: 每个fun输入为list中的一个元素，fun本身输入为一个元素
            + list_sample: 每个fun输入为list的子list，fun本身输入为一个list
    """
    if data_split == "avg":
        size = len(inputs) // k
        package_inputs = []
        for i in range(k):
            if i != k - 1:
                package_inputs.append(inputs[i * size: (1 + i) * size])
            else:
                package_inputs.append(inputs[i * size:])
    elif data_split == "raw":
        assert len(inputs) == k, f"[ERROR] inputs_len({len(inputs)}) != k({k})"
    else:
        all_num = len(inputs)
        splits = [float(i) for i in data_split.split(":") if i]
        assert len(splits) == k, f"[ERROR] data_split({len(splits)}) != k({k})"
        split_nums = [int(np.ceil(i / sum(splits) * all_num)) for i in splits]
        package_inputs = [inputs[:split_nums[0]]]
        for i in range(1, k):
            if i != k - 1:
                start = sum(split_nums[:i])
                end = start + split_nums[i]
                package_inputs.append(inputs[start:end])
            else:
                start = sum(split_nums[:i])
                package_inputs.append(inputs[start:])

    wrapped_fun = partial(fun_wrapper, fun=fun, fun_type=fun_type)
    m_res = multi_threading_execution(wrapped_fun, package_inputs, k)
    res = []
    for line in m_res:
        res.extend(line)
    return res


def parall_funs(funs, inputs):
    wrapped_funs = [partial(fun_wrapper, fun=fun, fun_type="list_sample") for fun in funs]
    m_res = multi_threading_execution_for_mutlifuncs(wrapped_funs, inputs)
    return m_res