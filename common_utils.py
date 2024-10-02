import os
import glob
import json
import time
import re
import datetime
import subprocess
import numpy as np
import pandas as pd
from typing import Union, List, Dict


def parse_path_list(paths: Union[str, List[str]]) -> List[str]:
    res = []
    if isinstance(paths, str):
        paths = glob.glob(paths)
        
    for path in paths:
        tmp_res = glob.glob(path)
        for p in tmp_res:
            if p not in res:
                res.append(p)
        
    return sorted(list(set(res)))


def load_csv_list(path_list: List[str], sep, nrows=None):
    if path_list:
        return pd.concat([pd.read_csv(p, sep=sep, nrows=nrows) for p in path_list]).reset_index(drop=True)
    else:
        raise FileNotFoundError("No files matching!")


def load_json_list(path_list: List[str]):
    if path_list:
        return pd.concat([pd.DataFrame(json.loads(p)) for p in path_list]).reset_index(drop=True)
    else:
        raise FileNotFoundError("No files matching!")


def load_csv(paths: Union[str, List[str]], sep: str, fmt="csv"):
    paths = parse_path_list(paths)
    print("Loading files: \n{}".format('\n'.join(paths)))
    if fmt == "csv":
        return load_csv_list(paths, sep)
    elif fmt == "json":
        return load_json_list(paths)
    else:
        raise Exception(f"[ERROR] Unknown fmt='{fmt}'")


def to_device(dict_tensors, device):
    if isinstance(dict_tensors, dict):
        result_tensors = {}
        for key, value in dict_tensors.items():
            result_tensors[key] = value.to(device)
        return result_tensors
    elif hasattr(dict_tensors, "to"):
        return dict_tensors.to(device)
    else:
        raise Exception(f"Data must be torch tensor. got '{dict_tensors}'")


def ddp_setup(rank, world_size):
    """
    Args:
        rank: 进程的唯一标识，在 init_process_group 中用于指定当前进程标识
        world_size: 进程总数
    """
    os.environ["MASTER_ADDR"] = "localhost"
    os.environ["MASTER_PORT"] = "80886"
    init_process_group(backend="nccl", rank=rank, world_size=world_size)
    torch.cuda.set_device(rank)


def load_csv(paths: Union[str, List[str]], sep: str, nrows=None):
    paths = parse_path_list(paths)
    print("[INFO] Loading files:")
    print_paths(paths)
    return load_json_list(paths, sep, nrows)


def load_csv(paths: Union[str, List[str]], sep: str, nrows=None):
    paths = parse_path_list(paths)
    print("[INFO] Loading files:")
    print_paths(paths)
    return load_csv_list(paths, sep, nrows)


def plot_history(history: Dict[str, List[Union[float, int, str]]], plot_keys_list: List[List[str]], save_root: str = "."):
    """打印并保存输出结果
    """
    import matplotlib.pyplot as plt 
    for plot_keys in plot_keys_list:
        plt.figure()
        plt.plot(pd.DataFrame({key: history[key] for key in plot_keys}))
        plt.legend(plot_keys)
        path = f"{save_root}/train_metrics.{'.'.join(plot_keys)}.jpg"
        plt.savefig(path)
        print(f"plot_keys={plot_keys} has been saved in '{path}'")
    
    h_string = json.dumps(dict([(k, [float(vi) for vi in v]) for k, v in history.items()]), indent=2)
    path = f"{save_root}/train_metrics.json"
    
    with open(path, 'w') as f:
        f.write(h_string)

    print(f"metrics has been saved in '{path}'")


def recurrent_read_dict(input_dict: dict, keys: str):
    """
    循环解析并读嵌套取字典key
        - 例如：
        >>> d = {"a": {"aa": {"aaa": {"aaaa": 2, "bbbb": 3}}}}
        >>> recurrent_read_dict(a, "a")
            [out] {'aa': {'aaa': {'aaa': 2, 'bbb': 3}}}
        >>> recurrent_read_dict(a, "a->aa->aaa->bbbb")
            [out] 3
    """
    tmp_key, past_key = keys.split("->")[0].strip(), "->".join(keys.split("->")[1:]).strip()
    if past_key:
        return recurrent_read_dict(input_dict[tmp_key], past_key)
    else:
        if tmp_key not in input_dict:
            # 20240227更新，支持最后一个key不存在，返回空，但是不能支持中间的某个key不存在。
            return ""
        else:
            return input_dict[tmp_key]


def read_gpt_res(path, keys_map):
    gpt_res = [json.loads(line) for line in open(path)]
    gpt_res_df = pd.DataFrame([
        {k: recurrent_read_dict(line, v) for k, v in keys_map.items()} for line in gpt_res
    ])
    return gpt_res_df


def read_gpt_res_list(path, keys_map):
    """
    path - 路径列表，支持读取多条路径
    """
    if path and isinstance(path, str) and os.path.exists(path):
        path_list = [path]
    elif isinstance(path, list):
        path_list = path
    else:
        raise Exception(f"[ERROR] Unknown type={type(path)}")

    if path_list:
        return pd.concat([read_gpt_res(path, keys_map) for path in path_list])
    else:
        raise Exception("No matching file!")


def print_paths(paths):
    print("[\n" +  "    " + "\n    ".join(paths) + "\n]")


def activate_hadoop_env(script: str = "/UserData/program/bashrc"):
    if os.path.exists(script):
        pipe = subprocess.Popen(". %s; env" % script, stdout=subprocess.PIPE, shell=True)
    else:
        raise FileNotFoundError(f"script: {script} not found")
    output = pipe.communicate()[0]
    env_new = {line.split("=")[0]: line.split("=")[1] for line in output.decode("utf-8").splitlines() if '=' in line and "==>" not in line}
    os.environ.update(env_new)
    print("HDFS:", os.environ.get(""))


def sample_querys(query_path, sample_nums):
    df = pd.read_csv(query_path, sep="\t", usecols=[0], name=["query"])
    df_sample = df.drop_dulicates().sample(sample_nums).reset_index(drop=True)
    df_sample.to_csv(query_path + ".sample", sep="\t", index=False, header=None)


def dropkey(x, keys):
    fathers, res_key = keys.split(".")[:-1], keys.split(".")[-1]
    for key in fathers:
        if key in x:
            x = x[key]
        else:
            return False
    if res_key in x:
        x.pop(res_key)
        return True
    else:
        return False


def dropkeys(x, keys):
    for k in keys:
        dropkey(x, k)


def today(fmt="%Y%m%d.%H%M"):
    return datetime.datetime.today().strftime(fmt)


def timestamp2str(timestamp, fmt="%Y%m%d %H:%M:%S"):
    time_local = time.localtime(int(timestamp))
    return time.strftime(fmt, time_local)


def sigmoid(x):
    return 1 / (1 + np.exp(-np.array(x)))


def split_str(string, seps):
    pattern = '|'.join(map(re.escape, seps))
    result = re.split(pattern, string)
    return result