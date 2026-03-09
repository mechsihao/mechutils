# -*- coding:utf-8 -*-
import os
import sys
import json
import time

try:
    from json import JSONDecodeError
except:
    JSONDecodeError = ValueError
    
import tqdm
try:
    import cv2
except:
    print("[WARNING] Not found cv2 package, please use `pip install opencv-python` to install.")
import base64

import numpy as np
import pandas as pd
import codecs

from PIL import Image, JpegImagePlugin
from io import BytesIO
from typing import Union, List

from glob import glob

from . import oss_util, hdfs_util, pangu_util, print_util, odps_util

if sys.version_info.major == 2:
    from .multi_processor_util_py2 import parall_fun, partial
    open = codecs.open
else:
    from .multi_processor_util import parall_fun, partial
    
from .common_utils import wget_data, glob_http, download_url_file

global CACHE_ROOT

_CACHE_ROOT = os.environ.get("MECH_CACHE_DIR", "/UserData/cache/")
_CACHE_ROOT = _CACHE_ROOT if os.path.exists(_CACHE_ROOT) else os.environ.get("MLLM_CACHE_DIR", "/apsarapangu/disk8/mech.lsh/cache/")
_CACHE_ROOT = _CACHE_ROOT if os.path.exists(_CACHE_ROOT) else os.environ.get("MLLM_CACHE_DIR", "/apsarapangu/disk4/mech.lsh/cache/")
_CACHE_ROOT = _CACHE_ROOT if os.path.exists(_CACHE_ROOT) else os.environ.get("MLLM_CACHE_DIR", "/apsarapangu/disk3/mech.lsh/cache/")
CACHE_ROOT = _CACHE_ROOT if os.path.exists(_CACHE_ROOT) else ".cache"


def init_cache_root(cache_root=None):
    global CACHE_ROOT
    if cache_root:
        CACHE_ROOT = cache_root
        mkdir(CACHE_ROOT)
        print("[INFO] CACHE_ROOT is changed to: {}".format(CACHE_ROOT))


def get_cache_path(remote_path=None):
    if remote_path:
        if remote_path.startswith("oss://") or remote_path.startswith("hdfs://") or remote_path.startswith("pangu://") or remote_path.startswith("http://") or remote_path.startswith("https://"):
            assert remote_path.count("/") >= 3, "[ERROR] Path depth error! oss save path must be `oss://bucket_name/root_dir_name/...`"
            prefix = remote_path.split(":")[0]
            return os.path.join(CACHE_ROOT, prefix, '/'.join(remote_path.split('/')[3:]))
        else:
            return remote_path
    else:
        return CACHE_ROOT


def isdir(path):
    if path.startswith("oss://"):
        assert globs(path), "[ERROR] {} dose not exist".format(path)
        res = os.popen("osscmd meta {}".format(path)).read()
        meta = {i.split(":")[0].strip(): i.split(":")[1].strip() for i in res.split("\n") if i}
        return int(meta['content-length']) == 0
    elif path.startswith("hdfs://"):
        return os.system("hdfs dfs -test -d {}".format(path)) == 0
    elif path.startswith("pangu://"):
        return os.system("pu ls {path}".format(path)) == 0
    elif path.startswith("https://") or path.startswith("http://"):
        raise Exception("[ERROR] http not support!")
    else:
        return os.path.isdir(path)


def mkdir(local_root):
    if len(local_root.split()) > 1:
        raise Exception("[ERROR] bad make dir: {local_root} ".format(local_root))
    if not os.path.exists(local_root) and os.system("mkdir -p %s" % local_root) != 0:
        raise Exception("[ERROR] System mkdir error, path=`{}`".format(local_root))


def file_exist(path):
    if path.startswith("oss://"):
        return len(oss_util.glob_oss(path)) > 0
    elif path.startswith("hdfs://"):
        return len(hdfs_util.glob_hdfs(path)) > 0
    elif path.startswith("pangu://"):
        return len(pangu_util.glob_pangu(path)) > 0
    elif path.startswith("http://") or path.startswith("https://"):
        raise Exception("")
    else:
        return os.path.exists(path)


def globs(path_pattern):
    if isinstance(path_pattern, str):
        if path_pattern.startswith("oss://"):
            return oss_util.glob_oss(path_pattern)
        elif path_pattern.startswith("hdfs://"):
            return hdfs_util.glob_hdfs(path_pattern)
        elif path_pattern.startswith("pangu://"):
            return pangu_util.glob_pangu(path_pattern)
        elif path_pattern.startswith("http://"):
            return glob_http(path_pattern)
        elif path_pattern.startswith("https://"):
            return glob_http(path_pattern)
        elif path_pattern.startswith("odps://"):
            return odps_util.globs_partition(path_pattern)
        else:
            return glob(path_pattern)
    else:
        raise Exception("[ERROR] Unknown type: {}".format(str(type(path_pattern))))


def abspath(path):
    tmp = os.path.abspath(path)
    if path.startswith("oss://"):
        return tmp[len(os.getcwd())+1:].replace("oss:/", "oss://")
    elif path.startswith("hdfs://"):
        return tmp[len(os.getcwd())+1:].replace("hdfs:/", "hdfs://")
    elif path.startswith("pangu://"):
        return tmp[len(os.getcwd())+1:].replace("pangu:/", "pangu://")
    else:
        return tmp


def download_data(data_path, read_cache, be_quiet=True, url_sleep=None):
    local_dir = get_cache_path(os.path.dirname(data_path))
    mkdir(local_dir)
    local_file = get_cache_path(data_path)
    if os.path.abspath(data_path) == os.path.abspath(local_file): 
        return local_file
    else:
        if data_path.startswith("oss://"):
            oss_util.download_file(data_path, local_file, read_cache=read_cache, be_quiet=be_quiet)
        elif data_path.startswith("hdfs://"):
            hdfs_util.download_file(data_path, local_file, read_cache=read_cache)
        elif data_path.startswith("pangu://"):
            pangu_util.download_file(data_path, local_file, read_cache=read_cache, be_quiet=be_quiet)
        elif data_path.startswith("http://") or data_path.startswith("https://"):
            try:
                wget_data(data_path, local_file, read_cache=read_cache, sleep_time=url_sleep)
            except:
                download_url_file(data_path, local_file)
        else:
            local_file = data_path
    return local_file


def upload_data(local_file, remote_file, safe_mode=False, be_quiet=True):
    if remote_file.startswith("oss://"):
        oss_util.upload_file(local_file, remote_file, be_quiet=be_quiet)
    elif remote_file.startswith("hdfs://"):
        hdfs_util.upload_file(local_file, remote_file)
    elif remote_file.startswith("pangu://"):
        if safe_mode:
            pangu_util.upload_file_safe(local_file, remote_file, max_retries=3, timeout=600, be_quiet=be_quiet)
        else:
            pangu_util.upload_file(local_file, remote_file, be_quiet=be_quiet)
    else:
        local_file = remote_file
    return local_file


def read_dataframe(path, header=0, sheet=0, sep="\t", doc_sep=None, nrows=None, fmt=None, skip_error=False, dtype=None, **kwargs):
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
    fmt = fmt or path.split("/")[-1].split(".")[-1]
    fmt = fmt.lower()
    if fmt == 'xlsx':
        sheets = [sheet] if not isinstance(sheet, list) else sheet
        df = pd.concat([pd.read_excel(path, header=_header, names=_names, sheet_name=sheet, nrows=nrows, dtype=dtype) for sheet in sheets]).reset_index(drop=True)
    elif fmt in ['json', 'jsonl']:
        try:
            raise
            df = pd.read_json(path, nrows=nrows, lines=True, encoding_errors="ignore")
        except:
            data = []
            with open(path, "r", encoding="utf-8", errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    if not line:  # 跳过空行
                        continue
                    try:
                        data.append(json.loads(line))
                        if nrows and len(data) == nrows:
                            break
                    except JSONDecodeError as e:
                        if skip_error:
                            print("Skipping `{}` invalid line: {}...".format(path.split('/')[-1], line.strip()[:10]))
                        else:
                            raise Exception("[ERROR] Read error for `{}`, detail: {}".format(path, str(e)))
            df = pd.DataFrame(data)
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
    condition = kwargs.get("condition", "")
    return df.query(condition) if condition else df


def read_file_single(data_path, header=0, sheet=0, sep="\t", doc_sep=None, read_cache=True, nrows=None, fmt=None, skip_error=False, be_quiet=True, **kwargs):
    local_file = download_data(data_path, read_cache, be_quiet=be_quiet)
    return read_dataframe(local_file, header, sheet, sep, doc_sep=doc_sep, nrows=nrows, fmt=fmt, skip_error=skip_error, **kwargs)


def read_file(paths, header=0, sheet=0, sep="\t", doc_sep=None, read_cache=True, nrows=None, fmt=None, work_num=16, skip_error=False, be_quiet=True, **kwargs):
    """通用读取接口，支持\t分割的csv、xlsx、parquet、pickle、json
    """
    start = time.time()
    if isinstance(paths, list):
        assert paths, "[ERROR] Got empty paths!"
        if work_num > 0:
            work_num = min(len(paths), work_num)
            if len(paths) > 1:
                parall_read = partial(read_file_single, header=header, sheet=sheet, sep=sep, doc_sep=doc_sep, read_cache=read_cache, nrows=nrows, fmt=fmt, skip_error=skip_error, be_quiet=be_quiet, **kwargs)
                res = parall_fun(parall_read, paths, k=work_num)
                df = pd.concat(res).reset_index(drop=True)
            else:
                df = read_file_single(paths[0], header, sheet, sep, doc_sep=doc_sep, read_cache=read_cache, nrows=nrows, fmt=fmt, skip_error=skip_error, be_quiet=be_quiet, **kwargs)
        else:
            df = pd.concat([read_file_single(p, header, sheet, sep, doc_sep=doc_sep, read_cache=read_cache, nrows=nrows, fmt=fmt, skip_error=skip_error, be_quiet=be_quiet, **kwargs) for p in paths]).reset_index(drop=True)
    elif isinstance(paths, str):
        df = read_file_single(paths, header, sheet, sep, doc_sep=doc_sep, read_cache=read_cache, nrows=nrows, fmt=fmt, skip_error=skip_error, be_quiet=be_quiet, **kwargs)
    else:
        raise Exception("[ERROR] Unknown type of input path, expect `str` or `List[str]`, got {}".format(str(type(paths))))
    end = time.time()
    if kwargs.get("show_log", True):
        print("[INFO] Got df data nums: {}, cost: {:.2f}s".format(len(df), end-start))
    return df


def write_text(df, tmp_path, sep, doc_sep):
    with open(tmp_path, "w") as f:
        for line in df.astype(str).values:
            f.write(sep.join(line) + doc_sep)


def dump_file(df, dump_path, header=True, sep="\t", doc_sep=None, suffix="", sheet="Sheet1", url_on=True, fmt=None, safe_upload=False, be_quiet=True):
    """ 
    通用保存接口，支持\t分割的csv、xlsx、parquet、pickle、json、pjson，支持保存到hdfs、oss、os
    """
    add_suffix = lambda x, s: ".".join(x.split(".")[:-1]) + s + "." + x.split(".")[-1] if "." in os.path.basename(x) else x + s 
    tmp_path = get_cache_path(dump_path)
    tmp_path = add_suffix(tmp_path, suffix)
    mkdir(os.path.dirname(tmp_path))
    dump_path = add_suffix(dump_path, suffix)
    fmt = fmt or dump_path.split(".")[-1]
    if fmt == "xlsx":
        if isinstance(df, pd.DataFrame):
            excel_sheets = {sheet: df}
        elif isinstance(df, dict):
            if isinstance(list(df.values())[0], pd.DataFrame):
                excel_sheets = df
            else:
                excel_sheets = {sheet: pd.DataFrame(df)}
        engine = "xlsxwriter"  # "openpyxl"
        with pd.ExcelWriter(tmp_path, engine=engine, engine_kwargs={'options':{'strings_to_urls': url_on}}) as writer:
            for tmp_sheet, tmp_df in excel_sheets.items():
                tmp_df.to_excel(writer, sheet_name=tmp_sheet, index=False)
    elif fmt == "json" or fmt == "jsonl":
        with open(tmp_path, 'w', encoding='utf-8') as f:
            for i in tqdm.tqdm(range(len(df)), desc="SAVING"):
                line = df.iloc[i].to_dict()
                line_str = json.dumps(line, ensure_ascii=False)
                f.write(line_str + "\n")
    elif fmt == "pjson" or fmt == "pure_json":
        df.to_json(tmp_path, orient="records", force_ascii=False, indent=2)
    elif fmt == "parquet":
        df.to_parquet(tmp_path)
    elif fmt == "pickle":
        df.to_pickle(tmp_path)
    else:
        if doc_sep and doc_sep != "\n":
            write_text(df, tmp_path, sep=sep, doc_sep=doc_sep)
        else:
            df.to_csv(tmp_path, sep=sep, index=False, header=header)
    upload_data(tmp_path, dump_path, safe_mode=safe_upload, be_quiet=be_quiet)
    print("[INFO] File save success: {}, cache path: {}".format(dump_path, tmp_path))


def read_prompt(prompt_path, read_cache=True, be_quiet=True):
    local_path = download_data(prompt_path, read_cache, be_quiet=be_quiet)
    return "".join([line for line in open(local_path) if not line.strip().startswith("//")])  # prompt 支持注释，用‘//’符号表示注释


def read_json(json_path, read_cache=True, be_quiet=True):
    local_path = download_data(json_path, read_cache, be_quiet=be_quiet)
    return json.loads(open(local_path).read())


def dump_json(data, json_path, indent=2, be_quiet=True):
    local_json_path = get_cache_path(json_path)
    with open(local_json_path, 'w') as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=indent))
    upload_data(local_json_path, json_path, be_quiet=be_quiet)


def record_nums(num, path):
    with open(path, 'w') as f:
        f.write(str(num))


def read_text(data_path, read_cache=True, be_quiet=True):
    local_file = download_data(data_path, read_cache, be_quiet=be_quiet)
    return open(local_file, errors='ignore').read()


def read_texts(paths, read_cache=True):
    """统一的读取文件接口: 
        每个文件会被读取成一个string
    Arguments:
        paths {[str, List[str]]} -- [path pattern or path or path list]
    Keyword Arguments:
        header {int} -- [file header, only for csv and xlsx] (default: {0})
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
        raise Exception("[ERROR] Unknown path, got {}".format(paths))
    return [read_text(p, read_cache=read_cache) for p in paths]


def dump_text(text, path, be_quiet=True):
    local_path = get_cache_path(path)
    mkdir(os.path.dirname(local_path))
    with open(local_path, 'w') as f:
        f.write(text)
    upload_data(local_path, path, be_quiet=be_quiet)


def read_df(paths, header=0, sheet=0, read_cache=True, sep="\t", doc_sep=None, nrows=None, fmt=None, work_num=16, skip_error=False, **kwargs):
    """
    ### Describe
    统一的读取文件接口:
        - 支持parquet、json、jsonl、csv、xlsx、pickle等格式的读取
        - 支持从hdfs、oss、pangu、本地直接读取
        - 支持通配符同时读取多个文件
 
    ### Arguments:
        - paths {[str, List[str]]} -- [path pattern or path or path list]
        - sheet {[str, int, List[str], List[int]]} -- [sheet id or sheet name, even list of them, it will concat list of them when sheet type is list, only for xlsx]
        - header {int} -- [file header, only for csv and xlsx] (default: {0})
        - read_cache {bool} -- [if `read_cache=False` then it will always download it from remote]
        - sep {str} -- [field sep]
        - doc_sep {str} -- [line sep]
        - work_num {int} -- [multi read to speed up, -1 is unable]
        - skip_error {bool} -- [it works for only read json data, if `true`, it will skip error lines]
        
    ### Keyword arguments:
        - condition {str} -- [read condition, example: "index in ['a', 'b', 'c']" or "title.str.startswith('A')" or "status == 'success'"]
        - show_log {bool} -- [whether to show matched files] (default: {True})
        
    ### Returns:
        - [pandas.DataFrame] -- [Union DataFrame]
    """
    if isinstance(paths, str):
        paths = globs(paths)
        if kwargs.get("show_log", True):
            print("[INFO] Match read files:")
            print_util.print_paths(paths)
    elif isinstance(paths, list):
        if kwargs.get("show_log", True):
            print("[INFO] Input read files:")
            print_util.print_paths(paths)
    else:
        raise Exception("[ERROR] Unknown path, got {}".format(paths))
    if kwargs.get("show_log", True):
        print("[INFO] Find {} files.".format(len(paths)))
    return read_file(paths, header, sheet, read_cache=read_cache, sep=sep, doc_sep=doc_sep, nrows=nrows, work_num=work_num, fmt=fmt, skip_error=skip_error, **kwargs)


def dump_df(df, dump_path, header = True, split_num=0, sep="\t", doc_sep="\n", url_on=True, sheet="Sheet1", fmt=None, safe_upload=False):
    """统一的保存文件接口:
        - 支持parquet、json、csv、xlsx、pickle等格式的保存
        - 支持直接写入到hdfs、oss、本地
        - 支持将文件均等切分存储成多个part

    Arguments:
        df {pd.DataFrame} -- [input dataFrame]
        dump_path {str} -- [dump path]

    Keyword Arguments:
        header {bool} -- [keep header or not] (default: {True})
        split_num {int} -- [split num] (default: {0})
    """
    if isinstance(df, list):
        df = pd.DataFrame(df)
        if header and isinstance(header, list) and isinstance(header[0], str):
            df.columns = header
    elif isinstance(df, str):
        df = pd.DataFrame([df], columns=["text"])
    
    if split_num <= 0:
        dump_file(df, dump_path, header, sep, doc_sep, url_on=url_on, sheet=sheet, fmt=fmt, safe_upload=safe_upload)
    else:
        n = len(df) // split_num
        inputs = []
        for i in range(split_num):
            if i == split_num - 1:
                tmp_df = df.iloc[i*n:]
            else:
                tmp_df = df.iloc[i*n:(i+1)*n]
            inputs.append([tmp_df, ".part" + str(i)])

        parall_dump = lambda x: dump_file(df=x[0], dump_path=dump_path, header=header, sep=sep, doc_sep=doc_sep, suffix=x[1], url_on=url_on, sheet=sheet, fmt=fmt, safe_upload=safe_upload)
        if len(inputs) == 1:
            parall_dump(inputs[0])
        else:
            work_num = min(len(inputs), 32)
            parall_fun(parall_dump, inputs, k=work_num)


def _read_im(img_path, return_type="bin", read_cache=True, read_func=None, not_exist="error", url_sleep=None, check_download_error=True, be_quiet=True):
    """读取Image
    Arguments:
        img_path {str} -- [支持5种路径：oss、hdfs、pangu、linux、url]
        read_func {func} -- [支持自己编写的外部传入的读取方式]
    Keyword Arguments:
        return_type {str} -- [
            返回的数据类型，共5类:
                - bin: 二进制文件
                - b64: base64编码
                - cv2: numpy array
                - pil: pillow object
                - show: 直接展示(需要在jupyter上)
            ] (default: {"bin"})
    """
    _BIN_TYPE, _B64_TYPE, _ARR_TYPE, _OBJ_TYPE, _SHW_TYPE = ["bin", "binary", "ascii"], ["b64", "base64"], ["cv2", "np", "arr", "array"], ["pil", "obj"], ["show", None]
    assert return_type in _BIN_TYPE + _B64_TYPE + _ARR_TYPE + _OBJ_TYPE, "[ERROR] Unknown return_type='{}', support: {}".format(return_type, _BIN_TYPE + _B64_TYPE + _ARR_TYPE + _OBJ_TYPE)
    match_data = globs(img_path)
    if len(match_data) == 0:
        if not_exist == "pass": 
            return None
        else:
            raise FileNotFoundError("[ERROR] {} dose not exist.".format(img_path))
    local_img = download_data(img_path, read_cache, be_quiet=be_quiet, url_sleep=url_sleep)
    if check_download_error:
        try:
            Image.open(local_img)
        except Exception as e:
            raise Exception("[ERROR] Image may be broken when downloading, path: {}, detail: {}".format(local_img, str(e)))
    if not local_img:
        if not_exist == "pass":
            return None
        else:
            raise FileNotFoundError("[ERROR] {} dose not exist or download img failed.".format(img_path))
    if read_func:
        ret = read_func(img_path)
    else:
        if return_type in _BIN_TYPE:
            ret = open(local_img, "rb").read()
        elif return_type in _B64_TYPE:
            ret = base64.b64encode(open(local_img, "rb").read()).decode()
        elif return_type in _ARR_TYPE:
            ret = cv2.imread(local_img)
        elif return_type in _OBJ_TYPE:
            ret = Image.open(local_img)
        elif return_type in _SHW_TYPE:
            ret = cv2.imshow(img_path, cv2.imread(local_img))
    return ret


def _dump_im(img, dump_path, be_quiet=True):
    """[存储Image]
    Arguments:
        img {str} -- 输入可以是二进制串也可以是base64编码
        dump_path {str} -- 仅支持jpg格式，如果后缀不是jpg，则会被强制加上jpg后缀
    """
    if not dump_path.endswith(".jpg"):
        print("[WARNING] Image just support to dump as jpg file, got `{}`, it was been fixed to `{}.jpg`".format(os.path.basename(dump_path), os.path.basename(dump_path)))
        dump_path += ".jpg"
    local_file = get_cache_path(dump_path)
    mkdir(os.path.dirname(local_file))
    if isinstance(img, str):
        with open(local_file, 'wb') as f: f.write(base64.b64decode(img))
    elif isinstance(img, bytes):
        with open(local_file, 'wb') as f: f.write(img)
    elif isinstance(img, np.ndarray):
        cv2.imwrite(local_file, img)
    elif isinstance(img, Image.Image):
        img.save(local_file)
    else:
        raise Exception("[ERROR] Bad img type to save: `{}`".format(type(img)))
    upload_data(local_file, dump_path, be_quiet=be_quiet)
    

def read_im(img_path, return_type="bin", read_cache=True, read_func=None, work_num=16, not_exist="error", url_sleep=None, check_download_error=True):
    """not_exist 近针对一个list中某几个样本如果不存在的情况，如果整个不存在则直接报错
    """
    if isinstance(img_path, str):
        paths = globs(img_path)
        assert globs(img_path), "[ERROR] match nothing!"
    elif isinstance(img_path, list):
        paths = img_path
    else:
        raise Exception("[ERROR] Bad type: {}".format(type(img_path)))
    
    assert not_exist in ["error", "pass"], "[ERROR] Params `not_exist` must be `error` or `pass` or `raise`"
    
    if len(paths) > 1:
        parall_read = lambda x: _read_im(x, return_type=return_type, read_cache=read_cache, read_func=read_func, not_exist=not_exist, url_sleep=url_sleep, check_download_error=check_download_error)
        res = parall_fun(parall_read, paths, k=min(work_num, len(paths)))
    elif len(paths) == 1:
        res = _read_im(paths[0], return_type=return_type, read_cache=read_cache, read_func=read_func, not_exist=not_exist, url_sleep=url_sleep, check_download_error=check_download_error)
    else:
        raise Exception("[ERROR] Match nothing!")
    
    if return_type != "show":
        return res
        

def dump_im(img, dump_path, work_num=16, be_quiet=True):
    """[存储图像统一接口，支持多图，支持多种存储路径]

    Arguments:
        img {[binary|base64|np.array]} -- [由read_im读取的格式]
        dump_path {[str|List[str]]} -- [支持多图并行存储]

    Keyword Arguments:
        work_num {int} -- [并行度] (default: {16})
    """
    if isinstance(img, list):
        if isinstance(dump_path, list):
            assert len(img) == len(dump_path), "[ERROR] Image size dose not equal dump_path size."
            dump_paths = dump_path
        elif dump_path.count("*") == 1:
            dump_paths = [dump_path.replace("*", str(i)) for i in range(len(img))]
        elif dump_path.count("*") > 1:
            raise Exception("[ERROR] Input dump_path must be list or a str with only one `*`, got {} *.".format(dump_path.count('*')))
        else:
            raise Exception("[ERROR] Input dump_path must be list or a str with only one `*`.")
        parall_dump = lambda x: _dump_im(x[0], x[1], be_quiet=be_quiet)
        parall_fun(parall_dump, list(zip(img, dump_paths)), k=min(work_num, len(dump_paths)))
    else:
        _dump_im(img, dump_path, be_quiet=be_quiet)


def _x2pil(image):
    type_tag = ""
    if isinstance(image, str):
        image, type_tag = Image.open(BytesIO(base64.b64decode(image))), "b64"
    elif isinstance(image, bytes):
        image, type_tag = Image.open(BytesIO(image)), "bin"
    elif isinstance(image, Image.Image):
        image, type_tag = image, "obj"
    else:
        raise Exception("[ERROR] Unknown type {}.".format(type(image)))
    return image, type_tag


def _pil2x(image, img_type="bin"):
    if img_type == "b64":
        img_buffer = BytesIO()
        image.save(img_buffer, format='JPEG')
        byte_data = img_buffer.getvalue()
        base64_str = base64.b64encode(byte_data)
        return base64_str.decode()
    elif img_type == "bin":
        bytesIO = BytesIO()
        image.save(bytesIO, format="JPEG")
        return bytesIO.getvalue()
    elif img_type in ["pil", "obj"]:
        return image
    else:
        raise Exception("[ERROR] Unknown type {}.".format(img_type))
    

def x2pil(images):
    return_one = False
    if not isinstance(images, list):
        images = [images]
        return_one = True
    res = [_x2pil(img) for img in images]
    return res[0] if return_one else res


def pil2x(images, img_type):
    return_one = False
    if not isinstance(images, list):
        images = [images]
        return_one = True
    res = [_pil2x(img, img_type) for img in images]
    return res[0] if return_one else res
    

def get_img_size(image):
    return x2pil(image)[0].size


def resize_imgs(images, image_resolution, resample=Image.BILINEAR):
    res = []
    for image in images:
        image, type_tag = x2pil(image)
        if max(image.width, image.height) > image_resolution:
            # image_resolution本来的意义是分辨率，但在这里是任意一边的长度
            resize_factor = image_resolution / max(image.width, image.height)
            width, height = int(image.width * resize_factor), int(image.height * resize_factor)
            image = image.resize((width, height), resample=resample)
        image = pil2x(image, type_tag)
        res.append(image)
    return res


def cal_imgs_token_num(imgs, a=2, K=14):
    if not isinstance(imgs, list):
        imgs = [imgs]
    def get_token_num(x): 
        h, w = get_img_size(x)
        return (h * w) / (a**2 * K**2)
    return int(sum([get_token_num(img) for img in imgs]))


def record(inputs, record_file):
    if not isinstance(inputs, list):
        inputs = [inputs]
    with open(record_file, 'a') as f:
        for line in inputs:
            if isinstance(line, dict):
                line = json.dumps(line, ensure_ascii=False)
            f.write(line+"\n")


def read_lines(path, nrows=None, read_cache=True, show_log=False):
    """一行一行的读取，返回list
    """
    df = read_df(path, fmt="csv", sep="`@@@@@`", header=None, nrows=nrows, read_cache=read_cache, show_log=show_log)
    return df[0].tolist()


def dump_lines(datas, path):
    """一行一行的保存，输入list
    """
    df = pd.DataFrame(data={0: datas})
    dump_df(df, path, header=False, sep="\t", doc_sep="\n")


if __name__ == "__main__":
    # a = read_im("oss://mechlsh/data/mllm/images/sample_100_eval.v2/0000e5a5991bfe72d9e03dd4f2c79551_*.jpg")
    # dump_im(a, "oss://mechlsh/data/mllm/images/service_online/0000e5a5991bfe72d9e03dd4f2c79551_*.jpg")
    paths = globs("oss://mechlsh/data/gaokao_agent/zhiyuan_analyse/agent_zhiyuan_list_wq_xr/*.in*")
else:
    if not os.path.exists(CACHE_ROOT):
        print("[WARNING] Cache path root dose not found! make it: {}".format(CACHE_ROOT))
        mkdir(CACHE_ROOT)
