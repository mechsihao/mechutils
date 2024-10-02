import os
import json
from typing import Optional, List, Dict, Any, Union
from glob import glob
import pandas as pd
from . import oss_util, hdfs_util, pangu_util, print_util
from .multi_processor_util import parall_fun, partial


def read_dataframe(path, header=0, sheet=0, sep="\t", doc_sep=None, nrows=None, fmt=None):
    """
    Arguments:
        path {[str]} -- [读取文件的路径，支持格式：xlsx、json[jsonl]、parquet、pickle、csv[其他格式]]

    Keyword Arguments:
        header {int|List[str]} -- [columns，仅对xlsx、csv格式有效，可以是数字代表第几行，可以是list，代表直接输入columns] (default: {0})
        sheet {int|str} -- [sheet名称，仅对xlsx] (default: {0})
    """
    if header is None:
        _header, _names = None, None
    elif isinstance(header, int):
        _header, _names = header, None
    elif isinstance(header, list):
        _header, _names = None, header
    else:
        raise Exception("[ERROR] header only support int or list, got {}.".format(str(type(header))))
    fmt = fmt.lower() or path.split("/")[-1].split(".")[-1].lower()
    if fmt == 'xlsx':
        sheets = [sheet] if not isinstance(sheet, list) else sheet
        df = pd.concat([pd.read_excel(path, header=_header, names=_names, sheet_name=sheet, nrows=nrows) for sheet in sheets]).reset_index(drop=True)
    elif fmt in ['json', 'jsonl']:
        try:
            df = pd.read_json(path, nrows=nrows)
        except:
            df = pd.DataFrame([json.loads(i) for i in open(path)])
            df = df.iloc[: nrows or len(df)]
    elif fmt == 'parquet':
        df = pd.read_parquet(path)
    elif fmt == 'pickle':
        df = pd.read_pickle(path)
    else:
        if doc_sep:
            data = [line.split(sep) for line in open(path, errors='ignore').read().split(doc_sep) if line.strip()]
            if _header:
                _names = data[_header]
                data = data[_header + 1:]
            df = pd.DataFrame(data, columns=_names)
        else:
            df = pd.read_csv(path, sep=sep, header=_header, names=_names, nrows=nrows)
    return df


def mkdir(local_root):
    if not os.path.exists(local_root) and os.system("mkdir -p %s" % local_root) != 0:
        raise Exception("[ERROR] System mkdir error, path=`{}`".format(local_root))


def read_file_single(data_path, header=0, sheet=0, local_root=".cache", sep="\t", doc_sep=None, read_cache=True, nrows=None, fmt=None):
    mkdir(local_root)
    local_root = os.path.join(local_root, '/'.join(os.path.dirname(data_path).split("/")[3:]))
    mkdir(local_root)
    local_file = os.path.join(local_root, os.path.basename(data_path))
    if data_path.startswith("oss://"):
        _, is_download = oss_util.download_file(data_path, local_file, read_cache=read_cache)
        if is_download:
            print("[INFO] 下载成功, oss_file: {}, local_file: {}\n".format(data_path, local_file), end="")
    elif data_path.startswith("hdfs://"):
        hdfs_util.download_file(data_path, local_file)
        print("[INFO] 下载成功, hdfs_file: {}, local_file: {}\n".format(data_path, local_file), end="")
    elif data_path.startswith("pangu://"):
        pangu_util.download_file(data_path, local_file)
        print("[INFO] 下载成功, pangu_file: {}, local_file: {}\n".format(data_path, local_file), end="")
    else:
        local_file = data_path
    return read_dataframe(local_file, header, sheet, sep, doc_sep=doc_sep, nrows=nrows, fmt=fmt)


def read_file(paths, header=0, sheet=0, cache_root=".cache", sep="\t", doc_sep=None, read_cache=True, nrows=None, fmt=None, work_num=16):
    """通用读取接口，支持\t分割的csv、xlsx、parquet、pickle、json
    """
    if isinstance(paths, list):
        assert paths, "[ERROR] Got empty paths!"
        if work_num > 0:
            work_num = min(len(paths), work_num)
            if len(paths) > 1:
                parall_read = partial(read_file_single, header=header, sheet=sheet, local_root=cache_root, sep=sep, doc_sep=doc_sep, read_cache=read_cache, nrows=nrows, fmt=fmt)
                res = parall_fun(parall_read, paths, k=work_num)
                df = pd.concat(res).reset_index(drop=True)
            else:
                df = read_file_single(paths[0], header, sheet, cache_root, sep, doc_sep=doc_sep, read_cache=read_cache, nrows=nrows, fmt=fmt)
        else:
            df = pd.concat([read_file_single(p, header, sheet, cache_root, sep, doc_sep=doc_sep, read_cache=read_cache, nrows=nrows, fmt=fmt) for p in paths]).reset_index(drop=True)
    elif isinstance(paths, str):
        df = read_file_single(paths, header, sheet, cache_root, sep, doc_sep=doc_sep, read_cache=read_cache, nrows=nrows, fmt=fmt)
    else:
        raise Exception("[ERROR] Unknown type of input path, expect `str` or `List[str]`, got {}".format(str(type(paths))))
    print("[INFO] Got dataframe, df data nums: {}".format(len(df)))
    return df


def write_text(df, tmp_path, sep, doc_sep):
    with open(tmp_path, "w") as f:
        for line in df.astype(str).values:
            f.write(sep.join(line) + doc_sep)


def dump_file(df, dump_path, header=True, cache_root=".cache", sep="\t", doc_sep=None, suffix="", sheet="Sheet1", url_on=True):
    """ 
    通用保存接口，支持\t分割的csv、xlsx、parquet、pickle、json，支持保存到hdfs、oss、os
    """
    add_suffix = lambda x, s: ".".join(x.split(".")[:-1]) + s + "." + x.split(".")[-1] if "." in os.path.basename(x) else x + s
    if not os.path.exists(cache_root) and os.system("mkdir -p %s" % cache_root) != 0:
        raise Exception("[ERROR] System mkdir error, path=`{}`".format(cache_root))

    basename = os.path.basename(dump_path)
    tmp_path = add_suffix(os.path.join(cache_root, basename), suffix)
    dump_path = add_suffix(dump_path, suffix)

    if dump_path.endswith(".xlsx"):
        engine = "openpyxl"  # "xlsxwriter"
        with pd.ExcelWriter(tmp_path, engine='xlsxwriter', engine_kwargs={'options':{'strings_to_urls': url_on}}) as writer:
            df.to_excel(writer, sheet_name=sheet, index=False)
    elif dump_path.endswith(".json") or dump_path.endswith(".jsonl"):
        df.to_json(tmp_path, orient='records', lines=True, force_ascii=False)
    elif dump_path.endswith(".parquet"):
        df.to_parquet(tmp_path)
    elif dump_path.endswith(".pickle"):
        df.to_pickle(tmp_path)
    else:
        if doc_sep:
            write_text(df, tmp_path, sep=sep, doc_sep=doc_sep)
        else:
            df.to_csv(tmp_path, sep=sep, index=False, header=header)

    if dump_path.startswith("oss://"):
        oss_util.upload_file(tmp_path, dump_path)
    elif dump_path.startswith("hdfs://"):
        hdfs_util.upload_file(tmp_path, dump_path)
    else:
        os.makedirs(os.path.dirname(dump_path), exist_ok=True)
        assert os.system("mv {} {}".format(tmp_path, dump_path)) == 0

    print("[INFO] File save success: {}, cache path: {}".format(dump_path, tmp_path))


def file_exist(path):
    if path.startswith("oss://"):
        return len(oss_util.glob_oss(path)) > 0
    elif path.startswith("hdfs://"):
        return len(hdfs_util.glob_hdfs(path)) > 0
    else:
        return os.path.exists(path)


def globs(path_pattern):
    if path_pattern.startswith("oss://"):
        return oss_util.glob_oss(path_pattern)
    elif path_pattern.startswith("hdfs://"):
        return hdfs_util.glob_hdfs(path_pattern)
    elif path_pattern.startswith("pangu://"):
        return pangu_util.glob_pangu(path_pattern)
    else:
        return glob(path_pattern)


def read_prompt(prompt_path):
    # prompt 支持注释，用‘//’符号表示注释
    return "".join([line for line in open(prompt_path) if not line.startswith("//")])


def record_nums(num, path):
    with open(path, 'w') as f:
        f.write(str(num))


def read_text(data_path, read_cache=True):
    mkdir(local_root)
    local_root = os.path.join(local_root, '/'.join(os.path.dirname(data_path).split("/")[3:]))
    mkdir(local_root)
    local_file = os.path.join(local_root, os.path.basename(data_path))
    if data_path.startswith("oss://"):
        _, is_download = oss_util.download_file(data_path, local_file, read_cache=read_cache)
        if is_download:
            print("[INFO] 下载成功, oss_file: {}, local_file: {}\n".format(data_path, local_file), end="")
        text = open(local_file).read()
    elif data_path.startswith("hdfs://"):
        hdfs_util.download_file(data_path, local_file)
        print("[INFO] 下载成功, hdfs_file: {}, local_file: {}\n".format(data_path, local_file), end="")
        text = open(local_file).read()
    elif data_path.startswith("pangu://"):
        pangu_util.download_file(data_path, local_file)
        print("[INFO] 下载成功, pangu_file: {}, local_file: {}\n".format(data_path, local_file), end="")
        text = open(local_file).read()
    else:
        text = open(local_file).read()
    return text


def read_texts(paths, read_cache=True):
    """统一的读取文件接口:
        每个文件会被读取成一个string

    Arguments:
        paths {[str, List[str]]} -- [path pattern or path or path list]

    Keyword Arguments:
        header {int} -- [file header, only for csv and xlsx] (default: {0})
        cache_root {str} -- [cache data root] (default: {".cache"})

    Returns:
        [List[str]] -- [Union Text List]
    """
    if isinstance(paths, str):
        paths = globs(paths)
        print("[INFO] Match read files:")
        print_util.print_paths(paths)
    elif isinstance(paths, list):
        print("[INFO] Input read files:")
        print_util.print_paths(paths)
    else:
        raise Exception("[ERROR] Known path, got {}".format(paths))
    return [read_text(p, read_cache=read_cache) for p in paths]


def dump_text(text, path):
    dump_df([str(text)], path)


def read_df(paths, header=0, sheet=0, cache_root=".cache", read_cache=True, sep="\t", doc_sep=None, nrows=None, fmt=None, work_num=16):
    """统一的读取文件接口:
        - 支持parquet、json、jsonl、csv、xlsx、pickle等格式的读取
        - 支持从hdfs、oss、pangu、本地直接读取
        - 支持通配符同时读取多个文件
 
    Arguments:
        paths {[str, List[str]]} -- [path pattern or path or path list]
        sheet {[str, int, List[str], List[int]]} -- [sheet id or sheet name, even list of them, it will concat list of them when sheet type is list, only for xlsx]
        header {int} -- [file header, only for csv and xlsx] (default: {0})
        cache_root {str} -- [cache data root] (default: {".cache"})
        read_cache {bool} -- [if `read_cache=False` then it will always download it from remote]
        sep {str} -- [field sep]
        doc_sep {str} -- [line sep]
        work_num {int} -- [multi read to speed up, -1 is unable]

    Returns:
        [pandas.DataFrame] -- [Union DataFrame]
    """
    if isinstance(paths, str):
        paths = globs(paths)
        print("[INFO] Match read files:")
        print_util.print_paths(paths)
    elif isinstance(paths, list):
        print("[INFO] Input read files:")
        print_util.print_paths(paths)
    else:
        raise Exception("[ERROR] Unknown path, got {}".format(paths))
    return read_file(paths, header, sheet, cache_root, read_cache=read_cache, sep=sep, doc_sep=doc_sep, nrows=nrows, work_num=work_num, fmt=fmt)


def dump_df(df, dump_path, header: Union[List[int], List[str], bool] = True, split_num=0, cache_root=".cache", sep="\t", doc_sep="\n", url_on=True, sheet="Sheet1"):
    """统一的保存文件接口:
        - 支持parquet、json、csv、xlsx、pickle等格式的保存
        - 支持直接写入到hdfs、oss、本地
        - 支持将文件均等切分存储成多个part

    Arguments:
        df {[pd.DataFrame]} -- [input dataFrame]
        dump_path {[str]} -- [dump path]

    Keyword Arguments:
        header {bool} -- [keep header or not] (default: {True})
        split_num {int} -- [split num] (default: {0})
        cache_root {str} -- [cache root] (default: {".cache"})
    """
    if isinstance(df, list):
        df = pd.DataFrame(df)
        if header and isinstance(header, list) and isinstance(header[0], str):
            df.columns = header
    elif isinstance(df, str):
        df = pd.DataFrame([df], columns=["text"])
    
    if split_num <= 0:
        dump_file(df, dump_path, header, cache_root, sep, doc_sep, url_on=url_on, sheet=sheet)
    else:
        n = len(df) // split_num
        inputs = []
        for i in range(split_num):
            if i == split_num - 1:
                tmp_df = df.iloc[i*n:]
            else:
                tmp_df = df.iloc[i*n:(i+1)*n]
            inputs.append([tmp_df, ".part" + str(i)])

        parall_read = lambda x: dump_file(df=x[0], dump_path=dump_path, header=header, cache_root=cache_root, sep=sep, doc_sep=doc_sep, suffix=x[1], url_on=url_on, sheet=sheet)
        if len(inputs) == 1:
            parall_read(inputs[0])
        else:
            work_num = min(len(inputs), 32)
            parall_fun(parall_read, inputs, k=work_num)
