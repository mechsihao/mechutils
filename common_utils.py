# -*- coding:utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import glob
import json
import time
import psutil
import datetime
import requests
import subprocess
import functools
import textwrap
import traceback
from tqdm import tqdm

import numpy as np
import pandas as pd

try:
    import jieba_fast.posseg as pseg
except:
    print("[WARNING] Not found jieba_fast package, please use `pip install jieba_fast` to install.")

from PIL import Image, ImageDraw, ImageFont
from functools import wraps
from threading import Lock
try:
    from concurrent import futures
except:
    print("[WARNING] Not found concurrent package, please use `pip install futures` to install.")
from difflib import SequenceMatcher



_executor = None


def _init_executor():
    if globals()['_executor'] is None:
        globals()['_executor'] = futures.ThreadPoolExecutor(1)


def timeout(seconds):
    """超时装饰器，用于限制service的输出时间，如果service输出太慢，则认为其连接失败，退出
    """
    _init_executor()
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            future = _executor.submit(func, *args, **kwargs)
            return future.result(timeout=seconds)
        return wrapper
    return decorator


def qpslimit(max_qps):
    """QPS限制装饰器，用于限制请求服务的QPS
    """
    interval = 1.0 / max_qps
    lock = Lock()
    def decorator(func):
        last_call_time = [0.0]
        @wraps(func)
        def wrapped(*args, **kwargs):
            with lock:
                current_time = time.time()
                elapsed_time = current_time - last_call_time[0]
                sleep_time = interval - elapsed_time
                if sleep_time > 0:
                    time.sleep(sleep_time)
                last_call_time[0] = time.time()
            return func(*args, **kwargs)
        return wrapped
    return decorator


def retry(retries=3, delay=None, final="raise", error_out=None):
    """重试请求装饰器，用于重试请求服务。
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    attempt += 1
                    print("[WARNING] Attempt {attempt} failed: {e}, func name: {func}".format(attempt=attempt, e=str(e), func=func.__name__))
                    if attempt < retries:
                        if delay:
                            time.sleep(delay*attempt)
                    else:
                        if final == "raise":
                            e_detail = traceback.format_exc()
                            raise Exception("\n{}\n[ERROR] All retry attempts failed, func name: {}.\ndetail: {}\n{}\n".format('=' * 30, func.__name__, e_detail, '=' * 30))
                        else:
                            return error_out
        return wrapper
    return decorator


def mem_info(pid=None):
    pid = pid or os.getpid() 
    print('当前进程的内存使用：%.4f GB' % (psutil.Process(pid).memory_info().rss / 1024 / 1024 / 1024))


def to_device(dict_tensors, device):
    if isinstance(dict_tensors, dict):
        result_tensors = {}
        for key, value in dict_tensors.items():
            result_tensors[key] = value.to(device)
        return result_tensors
    elif hasattr(dict_tensors, "to"):
        return dict_tensors.to(device)
    else:
        raise Exception("Data must be torch tensor. got '{dict_tensors}'".format(dict_tensors=dict_tensors))


def plot_history(history, plot_keys_list, save_root = "."):
    """打印并保存输出结果
    """
    import matplotlib.pyplot as plt 
    for plot_keys in plot_keys_list:
        plt.figure()
        plt.plot(pd.DataFrame({key: history[key] for key in plot_keys}))
        plt.legend(plot_keys)
        path = "{save_root}/train_metrics.{plot_keys}.jpg".format(save_root=save_root, plot_keys=".".join(plot_keys))
        plt.savefig(path)
        print("plot_keys={plot_keys} has been saved in '{path}'".format(plot_keys=".".join(plot_keys), path=path))
    
    h_string = json.dumps(dict([(k, [float(vi) for vi in v]) for k, v in history.items()]), indent=2)
    path = "{save_root}/train_metrics.json".format(save_root=save_root)
    
    with open(path, 'w') as f:
        f.write(h_string)

    print("metrics has been saved in '{path}'".format(path=path))


def fillna_last_content_by_col(df, col):
    res = []
    last_content = ""
    if not len(df):
        return df
    else:
        for line in df.to_dict("records"):
            if str(line[col]) != "nan":
                last_content = line[col]
            line[col] = last_content
            res.append(line)
        return pd.DataFrame(res)


def fillna_last_content_by_cols(df, cols):
    for col in cols:
        df = fillna_last_content_by_col(df, col)
    return df


def recurrent_read_dict(input_dict, keys):
    """
    循环解析并读嵌套取字典key
        - 例如：
        >>> d = {"a": {"aa": {"aaa": {"aaaa": 2, "bbbb": 3}}}}
        >>> recurrent_read_dict(a, "a")
            [out] {'aa': {'aaa': {'aaa': 2, 'bbb': 3}}}
        >>> recurrent_read_dict(a, "a.aa.aaa.bbbb")
            [out] 3
    """
    tag = "."
    tmp_key, past_key = keys.split(tag)[0].strip(), tag.join(keys.split(tag)[1:]).strip()
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
    gpt_res_df = pd.DataFrame([{k: recurrent_read_dict(line, v) for k, v in keys_map.items()} for line in gpt_res])
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
        raise Exception("[ERROR] Unknown type={}".format(type(path)))

    if path_list:
        return pd.concat([read_gpt_res(path, keys_map) for path in path_list])
    else:
        raise Exception("No matching file!")


def print_paths(paths):
    print("[\n" +  "    " + "\n    ".join(paths) + "\n]")


def activate_hadoop_env(script = "/UserData/program/bashrc"):
    if os.path.exists(script):
        pipe = subprocess.Popen(". %s; env" % script, stdout=subprocess.PIPE, shell=True)
    else:
        raise FileNotFoundError("script: {script} not found".format(script=script))
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


def now(fmt="%Y.%m.%d-%H:%M:%S", with_week=False):
    r = today(fmt)
    if with_week:
        r += ', 星期' + ['一', '二', '三', '四', '五', '六', '日'][time.localtime().tm_wday]
    return  r


def str_to_timestamp(time_str, format_str="%Y-%m-%d %H:%M:%S"):
    try:
        dt = time.strptime(time_str, format_str)
        # 将dt转化为时间戳
        timestamp = str(int(time.mktime(dt)))
        return timestamp
    except ValueError as e:
        raise ValueError("时间字符串格式不正确: {}".format(str(e)))


def sigmoid(x):
    return 1 / (1 + np.exp(-np.array(x)))


def split_str(string, seps):
    pattern = '|'.join(map(re.escape, seps))
    result = re.split(pattern, string)
    return result


def glob_http(url):
    response = requests.head(url)
    if response.status_code == 200:
        return [url]
    else:
        return []


def save_local_htag(htag, file):
    """将htag文件存储成隐藏文件
    """
    htag_file = os.path.join(os.path.dirname(file), "." + os.path.basename(file) + ".htag")
    if os.path.exists(htag_file):
        os.remove(htag_file)
    with open(htag_file, "w") as f:
        f.write(htag)


def read_local_htag(file):
    htag_file = os.path.join(os.path.dirname(file), "." + os.path.basename(file)) + ".htag"
    return [line for line in open(htag_file) if line][0] if os.path.exists(htag_file) else ""


def wget_data(file_url, local_file, read_cache=True, sleep_time=None):
    if os.path.isdir(local_file):
        basename = os.path.basename(file_url).split("?")[0]
        local_file = os.path.join(local_file, basename).replace("%2B", "+").replace("%20", "_").replace("%2F", "_")
    htag = read_local_htag(local_file)
    if read_cache and htag == file_url:
        print("[INFO] <%s> Remote file '%s' has been downloaded, do not action.\n" % (now(), local_file), end="")
    else:
        try:
            res = requests.get(file_url)
            if res.status_code == 200:
                with open(local_file, 'wb') as f:
                    f.write(res.content)
            else:
                raise Exception("[ERROR] Download url file failed, error code: {}".format(res.status_code))
        except Exception as e:
            raise Exception("[ERROR] Download url file failed, error detail: {}".format(str(e)))
        save_local_htag(file_url, local_file)
    if sleep_time:
        time.sleep(sleep_time)
    return local_file


def download_url_file(url, local_file, chunk_size=8192):
    """带进度条的文件下载函数
    """
    try:
        head_response = requests.head(url)
        total_size = int(head_response.headers.get('content-length', 0))
        response = requests.get(url, stream=True)
        response.raise_for_status()
        os.makedirs(os.path.dirname(local_file) if os.path.dirname(local_file) else '.', exist_ok=True)
        with open(local_file, 'wb') as f:
            if total_size > 0:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(local_file)) as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            else:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
        
        print("文件已下载: {}".format(local_file))
        return local_file
    except requests.exceptions.RequestException as e:
        print(f"下载失败: {e}")
        raise
    except Exception as e:
        print(f"保存文件失败: {e}")


def parse_primus_data(data):
    if isinstance(data, pd.DataFrame):
        df = data
    else:
        raise Exception("[ERROR] Unknown format for data, except 'str' or 'DataFrame', got '{}'".format(type(data)))
    drop_keys = ['finish_reason', '$prompt_file_id', 'usage']
    pass_keys = [i for i in df.passParams.iloc[0].keys() if i not in drop_keys and not i.startswith("$")]
    main_keys = ["prompt", "response", "model"]
    for key in pass_keys:
        df[key] = df.passParams.apply(lambda x: x[key])
    return df[main_keys + pass_keys]
    

def resize_img(img_path, image_resolution, resample=Image.NEAREST):
    image = Image.open(img_path)
    if max(image.width, image.height) > image_resolution:
        # image_resolution本来的意义是分辨率，但在这里是任意一边的长度
        resize_factor = image_resolution / max(image.width, image.height)
        width, height = int(image.width * resize_factor), int(image.height * resize_factor)
        image = image.resize((width, height), resample=Image.NEAREST)
        image.save(img_path + ".c{resample}.{image_resolution}.jpg".format(resample=resample, image_resolution=image_resolution))
    else:
        print("[WARNING] No need to compress.")


class dict_obj(dict):
    def __getattr__(self, key):
        if key not in self:
            raise Exception("[ERROR] Dict have no key: {key}.".format(key=key))
        else:
            value = self[key]
            if isinstance(value, dict):
                value = dict_obj(value)
            return value


def isname(single_word_string):
    pair_word_list = pseg.lcut(single_word_string)
    for eve_word, cixing in pair_word_list:
        if cixing == "nr":
            return True
    return False


def avg_cut_maxlen(x, maxlen):
    """该函数用于 给定一个maxlen来限制文章list的总长度，这时候用平均长度限制每篇文章会造成比较大的长度浪费。
    input: 
        - doc_dict: {id: doc{str}}
    output:
        - len_dict: {id: doc_len}
    """
    sort_lens = sorted([(k, len(v)) for k, v in x.items()], key=lambda x: x[1])
    len_dict = {}
    while sort_lens:
        avg_max_len = maxlen // len(sort_lens)
        lens = sort_lens.pop(0)
        cut_len = min(avg_max_len, lens[1])
        len_dict[lens[0]] = cut_len
        maxlen -= cut_len
    return len_dict


def generate_text_image(text, width, height, font_path="arial.ttf", font_size=30, text_color="black", bg_color="white", text_start=(0, 0)):
    """
    生成包含文本的图片，可以通过修改字体来增加样本难度
    参数:
        text (str): 要显示的文本。
        width (int): 图片的宽度（像素）。
        height (int): 图片的高度（像素）。
        font_path (str): 字体文件路径，默认为Arial。
        font_size (int): 字体大小，默认为30。
        text_color (str): 文本颜色，默认为黑色。
        bg_color (str): 背景颜色，默认为白色。
        text_start (tuple): 文本的起始坐标 (x, y)，默认为(0, 0)。
    返回:
        Image: 生成的图片对象。
    """
    try:
        font = ImageFont.truetype(font_path, font_size)
    except IOError:
        print("字体文件未找到，使用默认字体。")
        font = ImageFont.load_default()
    image = Image.new("RGB", (width, height), color=bg_color)
    draw = ImageDraw.Draw(image)
    lines = textwrap.wrap(text, width=width // font_size)  # 根据图片宽度和字体大小估算每行字符数
    text_x, text_y = text_start  # 获取文本起始坐标
    for line in lines:
        draw.text((text_x, text_y), line, font=font, fill=text_color)
        text_y += font.getsize(line)[1]  # 更新下一行的起始位置
    return image


def chunk_text(text, chunk_size=4096):
    paragraphs = text.split('\n')
    chunks = []
    current_chunk = ""
    for paragraph in paragraphs:
        if len(paragraph) > chunk_size:
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = ""
            chunks.append(paragraph)
        else:
            if len(current_chunk) + len(paragraph) <= chunk_size:
                if current_chunk:
                    current_chunk += '\n' + paragraph
                else:
                    current_chunk = paragraph
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = paragraph
    if current_chunk:
        chunks.append(current_chunk)
    return [i+"\n" for i in chunks]


def en_ratio(text):
    """统计文本中所有字符的数量
    """
    total_chars = len(text)
    if total_chars == 0:
        return 0
    english_chars = 0
    for char in text:
        if char.isalpha() and ('a' <= char.lower() <= 'z'):
            english_chars = english_chars + 1
    ratio = english_chars / total_chars
    return ratio


def time_point(name="", notes=""):
    if "__time_points__" not in globals():
        global __time_points__
        __time_points__ = []
    current = time.time()
    last = globals()["__time_points__"][-1]["current"] if globals()["__time_points__"] else current
    last_name = globals()["__time_points__"][-1]["name"] if globals()["__time_points__"] else "__init__"
    tmp_points = {"index": len(globals()["__time_points__"]), "name": name, "current": current, "cost": current - last, "chain": "[{last_name}]->[{name}]".format(last_name=last_name, name=name), "notes": notes}
    globals()["__time_points__"].append(tmp_points)


def get_time_points():
    if "__time_points__" not in globals():
        global __time_points__
        __time_points__ = []
    return pd.DataFrame(globals()["__time_points__"], columns=["index", "name", "current", "cost", "chain", "notes"])


def get_cost(start, end):
    df = get_time_points()
    start_df = df.query("name == '{start}'".format(start=start))
    end_df = df.query("name == '{end}'".format(end=end))
    if start_df.shape[0] and end_df.shape[0]:
        return end_df.current.iloc[0] - start_df.current.iloc[0]


def clear_time_points():
    global __time_points__
    __time_points__ = []


def update_dict(old, new, dtype="num"):
    for k, v in new.items():
        if k in old:
            old_v = old[k]
            if isinstance(old_v, list):
                old[k] = list(set(old_v + v))
            else:
                old[k] = old_v + v
        else:
            old[k] = v
    return old


def check_hallucination(answer, refs, answer_k=10, ref_k=100, over_lap=20, error_thr=0.5):
    """
    校验答案中的内容是否能在参考文献中找到引用源
    
    参数:
    answer (str): 待校验的答案文本
    refs (list[str]): 参考文献列表，每个元素为一篇文献的文本
    k (int): 切分后每个句子的最大长度
    over_lap (int): 相邻句子之间的重叠长度
    
    返回:
    dict: 包含答案中每个子串及其可能的引用源信息
    """
    punctuation_pattern = r'[，。、；！？,.?!;]'
    
    def split_text(text, k):
        """将文本按标点符号切分，再按最大长度和重叠长度进一步切分"""
        sentences = re.split(punctuation_pattern, text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        result = []
        for sentence in sentences:
            result.append(sentence)
            # if len(sentence) <= k:
            #     result.append(sentence)
            # else:
            #     start = 0
            #     while start < len(sentence):
            #         end = min(start + k, len(sentence))
            #         while end > start and not sentence[end-1].isspace():
            #             end -= 1
            #         if end <= start:
            #             end = min(start + k, len(sentence))
                    
            #         result.append(sentence[start:end].strip())
            #         next_start = start + (end - start) - over_lap
            #         if next_start <= start:
            #             next_start = start + 1
            #         start = next_start
        
        return result
    
    answer_parts = split_text(answer, answer_k)
    ref_parts = []
    for doc_id, doc in enumerate(refs, 1):
        doc_sentences = split_text(doc, ref_k)
        for sent_id, sentence in enumerate(doc_sentences):
            ref_parts.append((doc_id, sent_id, sentence))
    result = {}
    for answer_part in answer_parts:
        if not answer_part:
            continue
            
        sources = []
        for doc_id, sent_id, ref_sentence in ref_parts:
            matcher = SequenceMatcher(None, answer_part, ref_sentence)
            match = matcher.find_longest_match(0, len(answer_part), 0, len(ref_sentence))
            if match.size >= len(answer_part) * error_thr:
                sources.append("文章{doc_id}: {ref_sentence}".format(doc_id=doc_id, ref_sentence=ref_sentence))
        
        result[answer_part] = sources
    
    return result


def check_and_fix_data(name, df, colstr, dtypes, default_value=None, error_limit_rate=None, check_keys=None, valid_fun=None, invalid_fun=None):
    """
    DataFrame的数据校验函数，通常用于线上数据的基本审核，主要校验的点有如下三个：
    - 1. 数据类型
    - 2. 数据合法性: 合法性由数据类型、是否为NaN、是否为None、是否不满足valid_fun、是否满足invalid_fun、check_keys是否存在 来决定。
    - 3. 不合法在阈值范围内过滤，如果超过阈值则报错。
    - 4. 如果给默认值，则对于不合法数据，直接使用默认值，如果不给默认值，则丢弃
    """
    if ':' in colstr:
        col, col_type = colstr.split(":")[0].strip(), colstr.split(":")[1].strip()
    else:
        col, col_type = colstr, None
        
    def trans_type(x):
        try:
            if col_type is not None:
                return eval(col_type)(x)
            else:
                return x
        except:
            return x
        
    df[col] = df[col].apply(trans_type)
         
    assert len(df) != 0, "[ERROR] {name}.{col} 为空，请检查".format(name=name, col=col)
    check_keys = check_keys or []
    error_limit_rate = error_limit_rate or 0.05
    dtypes = dtypes if isinstance(dtypes, list) else [dtypes]

    if default_value is None:
        assert df[df[col].fillna("").apply(lambda x: (x==-1) if isinstance(x, int) or isinstance(x, float) else (not x))].shape[0] == 0, "[ERROR] {name}.{col} 无默认值，不允许出现: 数值为-1、字符串为空、缺失字段、字段为None的情况".format(name=name, col=col)

    def check_status(x):
        if not any([isinstance(x, dtype) for dtype in dtypes]):
            return "TypeError"
        if valid_fun is not None and not valid_fun(x):
            return "IsNotValid"
        if invalid_fun is not None and invalid_fun(x):
            return "IsInvalid"
        if x is None:
            return "None"
        if isinstance(x, float) and pd.isna(x):
            return "NaN"
        if isinstance(x, dict):
            line = x
            for check_key in check_keys:
                check_key = check_key.split(":")[0]
                if check_key not in line:
                    return "MissKey"
        elif isinstance(x, list):
            for line in x:
                if isinstance(line, dict):
                    for check_key in check_keys:
                        check_key = check_key.split(":")[0]
                        if check_key not in line:
                            return "MissKey"
        return ""

    def get_status(x):
        if x == "":
            return "keep"
        else:
            if default_value is None:
                return "discard"
            else:
                return "fix"
            
    def trans_list_kv_type(x):
        if isinstance(x, dict):
            line = x
            for check_key in check_keys:
                eles = check_key.split(":")
                if len(eles) == 2:
                    _check_key, trans_type = eles
                else:
                    _check_key, trans_type = eles[0], None
                line[_check_key] = eval(trans_type)(line[_check_key]) if trans_type else line[_check_key]
        elif isinstance(x, list):
            for line in x:
                if isinstance(line, dict):
                    for check_key in check_keys:
                        eles = check_key.split(":")
                        if len(eles) == 2:
                            _check_key, trans_type = eles
                        else:
                            _check_key, trans_type = eles[0], None
                        line[_check_key] = eval(trans_type)(line[_check_key]) if trans_type else line[_check_key]
        return x
    
    df['msg'] = df[col].apply(check_status)
    df['status'] = df['msg'].apply(get_status)
    print("[CHECK ERROR] {name}.{col} 非法数据统计:".format(name=name, col=col))
    print(df.query("msg != ''")['msg'].value_counts())
    print("[CHECK DISCARD] {name}.{col} 丢弃(discard)/修正(fix)据统计:".format(name=name, col=col))
    print(df['status'].value_counts())
    error_rate = df.query("status in ['discard', 'fix']").shape[0] / (len(df) + 1e-5)

    if error_rate > error_limit_rate:
        raise Exception("[ERROR] {name}.{col} 校验不通过，数据非法率过高={error_rate:.2f}% 超过阈值{error_limit_rate:.2f}%".format(name=name, col=col, error_rate=error_rate*100, error_limit_rate=error_limit_rate*100))

    df = df.query("status != 'discard'")
    df[col] = df.apply(lambda x: default_value if x.status == 'fix' else x[col], axis=1)
    df[col] = df[col].apply(trans_list_kv_type)
    df = df.drop(["msg", "status"], axis=1)
    return df


def copy_jueying_data(name, jueying_input_data, jueying_output_data, columns=None):
    from .read_util import read_df, globs, dump_df
    print("[INFO] {name} 绝影文件保存，{jueying_input_data} -> {jueying_output_data}".format(name=name, jueying_input_data=jueying_input_data, jueying_output_data=jueying_output_data))
    df = read_df(jueying_input_data)
    df = pd.DataFrame(df.T[0].tolist())
    if columns:
        df.columns = columns
    dump_df(df, jueying_output_data)
    print("[INFO] {name} 保存成功".format(name=name))


def split_list_by_part(data_list, batch_part, split_type=1):
    """
    按顺序将list切分成多个部分，返回指定的部分
    
    Args:
        data_list: 要切分的list
        batch_part: current_part/total_part_num = 当前要获取的部分编号 (从1开始)/总共要切分成的部分数
    
    Returns:
        切分后的list的一部分
    """
    current_part, total_part_num = batch_part.split("/")
    current_part, total_part_num = int(current_part), int(total_part_num)
    if current_part > total_part_num or current_part <= 0:
        raise ValueError("current_part必须在[1, {total_part_num}]范围内".format(total_part_num=total_part_num))
    
    if total_part_num <= 0:
        raise ValueError("total_part_num必须大于0")
    
    current_part -= 1
    total_length = len(data_list)
    
    if split_type == 1:
        # 1为顺序切分[1~10]->[1,2,3], [4,5,6], ...
        base_size = total_length // total_part_num
        remainder = total_length % total_part_num
        if current_part < remainder:
            start_idx = current_part * (base_size + 1)
            end_idx = start_idx + base_size + 1
        else:
            start_idx = remainder * (base_size + 1) + (current_part - remainder) * base_size
            end_idx = start_idx + base_size
        return data_list[start_idx:end_idx]
    else:
        # 2为隔断切分[1~10]->[1,4,7], [2,5,8], ...
        return data_list[current_part::total_part_num]


def safe_u8(s):
    """安全转换utf-8编码，用于python2中的中文字符串处理
    - 输入为字符串，输出为unicode
    - NOTE: **强烈建议**在自己编写的UDF中，**所有中文涉及到的字符串都用这个函数处理一次**，不然很容易在executor报错：ascii codec can't decode byte 0xe6 in position 0: ordinal not in range(128)
    """
    if isinstance(s, unicode):
        return s
    elif isinstance(s, str):
        try:
            return s.decode("utf-8")
        except UnicodeDecodeError:
            return s.decode("utf-8", "ignore")
    else:
        return unicode(s)