# -*- coding:utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import json
import re
import numpy as np

try:
    from wcwidth import wcswidth
    def _len(text):
        return wcswidth(text)
except:
    print("[WARNING] wcwidth not installed, use default _len function, please install wcwidth: pip3 install wcwidth")
    def _len(text):
        """
        计算字符串在Linux终端中的实际打印长度（不包括ANSI转义序列）
        参数: text: 输入字符串，可能包含ANSI颜色代码
        返回: int:  实际显示的字符长度
        """
        if not isinstance(text, str):
            text = str(text)
        ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
        cleaned_text = ansi_escape.sub('', text)
        length = 0
        for char in cleaned_text:
            code_point = ord(char)
            if (0x4e00 <= code_point <= 0x9fff or    # CJK统一汉字
                0x3400 <= code_point <= 0x4dbf or    # CJK扩展A
                0x20000 <= code_point <= 0x2a6df or  # CJK扩展B
                0xf900 <= code_point <= 0xfaff or    # CJK兼容汉字
                0xff00 <= code_point <= 0xffef or    # 全角ASCII、全角标点
                0x3000 <= code_point <= 0x303f or    # CJK标点符号
                0x3040 <= code_point <= 0x309f or    # 日文平假名
                0x30a0 <= code_point <= 0x30ff):     # 日文片假名
                length += 2  # CJK字符通常占2个字符宽度
            else:
                length += 1  # 其他字符占1个字符宽度
        return length


def print_line(line, max_len, prefix="", sep="", suffix="", gap=""):
    if line[0][0] == "=":
        sep = "=" * _len(sep)
    s = prefix + gap + sep.join([' ' * (max_len - _len(i)) + str(i) for i in line]) + gap + suffix
    print(s)


def print_metric(metric):
    """
    打印模型评估指标
    :param metric: 输入的矩阵，第一列必须为指标名称
    """
    metric_name_col = metric.columns[0]
    metric[metric_name_col] = metric[metric_name_col].astype(str)

    for col in metric.columns[1:]:
        metric[col] = metric[col].apply(lambda x: str(round(x, 4)) if isinstance(x, float) else ','.join(x) if isinstance(x, list) else str(x))

    max_len = max([_len(i) for i in metric.values.reshape(-1)]) + 2
    _, w = metric.shape

    print_line(["═" * max_len] * w, max_len, prefix="╔", sep="╦", suffix="╗", gap="")
    print_line([i.upper() for i in metric.columns], max_len, prefix="║", sep="║", suffix="║", gap="")
    print_line(["═" * max_len] * w, max_len, prefix="╠", sep="╬", suffix="╣", gap="")
    for i, line in enumerate(metric.values):
        print_line(line, max_len, prefix="║", sep="║", suffix="║", gap="")
    print_line(["═" * max_len] * w, max_len, prefix="╚", sep="╩", suffix="╝", gap="")


def print_mat(name, mat):
    print(name)
    shape = mat.shape
    if _len(shape) == 1:
        print(mat)
    else:
        for line in mat:
            print(np.around(line, 4))
    print()


def print_metrix(mat_name, input_metrix):
    """
    打印矩阵
    :param mat_name: 名称
    :param input_metrix: 输入的矩阵
    """
    metrix = np.array([round(i, 4) for i in input_metrix.reshape(-1)]).reshape(input_metrix.shape)
    if _len(metrix.shape) == 1:
        p_metrix = metrix[None, :]
        l, w = p_metrix.shape
    elif _len(metrix.shape) == 2:
        p_metrix = metrix
        l, w = p_metrix.shape
    else:
        d_end = metrix.shape[-1]
        d_end_2 = metrix.shape[-2]
        t_metrix = metrix.reshape(-1, d_end)
        l, w = t_metrix.shape
        max_len = max([max([_len(i) for i in line]) for line in t_metrix])
        res = []
        for i, line in enumerate(t_metrix):
            if i % d_end_2 == 0 and i != 0 and i != l:
                res.append(["=" * max_len] * w)
            res.append(line)
        p_metrix = np.array(res)

    max_len = max([_len(i) for i in p_metrix.reshape(-1)])
    print(mat_name)
    for line in p_metrix:
        print_line(line, max_len, prefix="[", sep=", ", suffix="]", gap="")
    print()


def print_series(name, series):
    series_dict = series.to_dict()
    keys = list(series_dict.keys())
    values = list(series_dict.values())
    max_key_len = max(max([_len(str(i)) for i in keys]), 5)
    max_value_len = max(max([_len(str(i)) for i in values]), 8)
    print(str(name))
    print("┏" + "━" * (max_key_len + 2) + "┳" + "━" * (max_value_len + 2) + "┓")
    print("┃" + " " * (max_key_len - 2) + "KEY ┃ VALUE" + " " * (max_value_len - 4) + "┃")
    print("┣" + "━" * (max_key_len + 2) + "╋" + "━" * (max_value_len + 2) + "┫")
    for key, value in zip(keys, values):
        print("┃ " + " " * (max_key_len - _len(str(key))) + "{} ┃ {}".format(key, value) + " " * (max_value_len - _len(str(value))) + " ┃")
    print("┗" + "━" * (max_key_len + 2) + "┻" + "━" * (max_value_len + 2) + "┛")


def print_dataframe_line(lines, index, max_index_len, max_key_lens, start, sep, end, blank):
    for i in range(_len(lines) + 1):
        content = lines[i - 1] if i >= 1 else " "
        if i == 0:
            print(start + blank + index + blank * (max_index_len - _len(index) + 1), end="")
        elif i == _len(lines):
            print(sep + blank + content + blank * (max_key_lens[i - 1] - _len(content) + 1) + end)
        else:
            print(sep + blank + content + blank * (max_key_lens[i - 1] - _len(content) + 1), end="")


def print_args_info(args_dict):
    """
    用于打印Dict类型输入的参数
    :param args_dict:
    :return:
    """
    keys = list(args_dict.keys())
    values = list(args_dict.values())
    print("Args Comments:")

    def get_type_value(v):
        if "[" in v and "]" in v:
            return v[v.index("["): v.index("]")].strip("[").strip("]").strip(), v[v.index("]") + 1:].strip()
        else:
            return str(type(v)).split("'")[1].strip(), v

    type_max_len = max([_len(get_type_value(i)[0]) for i in values]) + 2
    get_type_str = lambda x: str(" " * ((type_max_len - _len(x) + 1) // 2) + x + " " * ((type_max_len - _len(x)) // 2))[:type_max_len]

    max_key_len = max([_len(i) for i in keys])
    max_value_len = max([_len(get_type_value(str(i))[1]) for i in values])

    print("┏" + "━" * (max_key_len + 2) + "┳" + "━" * type_max_len + "┳" + "━" * (max_value_len + 2) + "┓")
    print("┃" + " " * (max_key_len - 4) + "ARG   ┃" + get_type_str("TYPE") + "┃   COMMENTS" + " " * (max_value_len - 9) + "┃")
    print("┣" + "━" * (max_key_len + 2) + "╋" + "━" * type_max_len + "╋" + "━" * (max_value_len + 2) + "┫")
    for key, value in zip(keys, values):
        t, value = get_type_value(value)
        print("┃ " + " " * (max_key_len - _len(key)) + "{} ┃".format(key) + get_type_str(t) + "┃ {}".format(value) + " " * (
                max_value_len - _len(str(value))
        ) + " ┃")
    print("┗" + "━" * (max_key_len + 2) + "┻" + "━" * type_max_len + "┻" + "━" * (max_value_len + 2) + "┛")


def get_args_table_str(args_dict, show_type=True):
    keys = list(args_dict.keys())
    values = [i.__name__ if callable(i) else i for i in list(args_dict.values())]
    types = {k: str(type(v)).split("'")[1].strip() for k, v in args_dict.items()}

    max_key_len = max(max([_len(i) for i in keys]), 10)
    max_value_len = max(max([_len(str(i)) for i in values]), 15)
    ret = []
    print("Args Table:")
    if show_type:
        type_max_len = 6
        get_type_str = lambda tp: str(" " * ((type_max_len - _len(tp) + 1) // 2) + tp + " " * ((type_max_len - _len(tp)) // 2))[:type_max_len]
        ret.append("┏" + "━" * (max_key_len + 2) + "┳" + "━" * type_max_len + "┳" + "━" * (max_value_len + 2) + "┓")
        ret.append("┃" + " " * (max_key_len - 4) + "ARG   ┃" + get_type_str("TYPE") + "┃   VALUE" + " " * (max_value_len - 6) + "┃")
        ret.append("┣" + "━" * (max_key_len + 2) + "╋" + "━" * type_max_len + "╋" + "━" * (max_value_len + 2) + "┫")
        for key, value in zip(keys, values):
            type_str = types[key]
            t = type_str if type_str != "function" else "func"
            ret.append("┃ " + " " * (max_key_len - _len(key)) + "{} ┃".format(key) + get_type_str(t) + "┃ {}".format(value) + " " * (
                    max_value_len - _len(str(value))
            ) + " ┃")
        ret.append("┗" + "━" * (max_key_len + 2) + "┻" + "━" * type_max_len + "┻" + "━" * (max_value_len + 2) + "┛")
    else:
        ret.append("┏" + "━" * (max_key_len + 2) + "┳" + "━" * (max_value_len + 2) + "┓")
        ret.append("┃" + " " * (max_key_len - 4) + "ARG   ┃   VALUE" + " " * (max_value_len - 6) + "┃")
        ret.append("┣" + "━" * (max_key_len + 2) + "╋" + "━" * (max_value_len + 2) + "┫")
        for key, value in zip(keys, values):
            ret.append("┃ " + " " * (max_key_len - _len(key)) + "{} ┃ {}".format(key, value) + " " * (max_value_len - _len(str(value))) + " ┃")
        ret.append("┗" + "━" * (max_key_len + 2) + "┻" + "━" * (max_value_len + 2) + "┛")
    return "\n".join(ret)


def print_args_table(args, show_type=True):
    """
    用于打印argparse的输入参数
    :param args:
    :param show_type:
    :return:
    """
    print(get_args_table_str(args.__dict__, show_type))


def print_json(args_dict):
    res = json.dumps(args_dict, indent=2, ensure_ascii=False)
    print(res)


def print_paths(paths, max_show_num=None, print_fun=print):
    n = len(paths)
    if max_show_num is not None:
        if len(paths) > max_show_num:
            paths = paths[:max_show_num-1] + [" ... {} files left ... ".format(len(paths) - max_show_num)] + paths[-1:]
    msg = "[\n" +  "    " + "\n    ".join(paths) + "\n] (total files num: {})".format(n)
    if print_fun is None:
        return msg
    else:
        print_fun(msg)


def simplify_data(d, keep_len=200000):
    keep = keep_len // 2
    packupstr = lambda v: v[:keep] + "\n ... ({} chars) ... \n".format(_len(v[keep:-keep])) + v[-keep:] if _len(v) > keep_len else v
    def __simp(d):
        if isinstance(d, str):
            return packupstr(d)
        elif isinstance(d, dict):
            return {k: __simp(v) for k, v in d.items()}
        elif isinstance(d, list):
            return [__simp(i) for i in d]
        else:
            return d
    return __simp(d)


def print_req(req, print_fun=print, keep_len=1000):
    """专门用于服务中请求体的打印，可以缩减打印字数，keep_len大于0即可将内部的value的文本字数保持在keep_len个
    """
    res = simplify_data(req, keep_len) if keep_len > 0 else req
    print_fun(json.dumps(res, ensure_ascii=False, indent=2))


def print_reranking(a, b, a_infos=None):
    # 将输入的字符串序列转换为列表
    a_list = a.split('>') if isinstance(a, str) else a
    b_list = b.split('>') if isinstance(b, str) else b
    a_infos = a_infos or [""] * len(a_list)
    assert len(a_list) == len(b_list) and len(a_list) == len(a_infos)
    original_indices = {element: idx for idx, element in enumerate(a_list)}
    reordered_indices = {element: idx for idx, element in enumerate(b_list)}
    a_infos = [[str(info)] if isinstance(info, str) else list([str(i).strip() for i in info]) for info in a_infos]
    max_len_list = np.array([[_len(j) for j in info] for info in a_infos]).max(0)

    changes = []
    for i, elem in enumerate(a_list):
        info = a_infos[i]
        original_idx = original_indices[elem]
        new_idx = reordered_indices[elem]
        # 计算变化的位置数
        shift = original_idx - new_idx
        
        if shift > 0:
            direction = "↑{shift}".format(shift=shift)  # 向上或前移
        elif shift < 0:
            direction = "↓{-shift}".format(shift=shift)  # 向下或后移
        else:
            direction = '─ '  # 位置未变
        elem = elem + max(8 - len(elem), 0) * " "
        if info:
            gap = max(4 - len(str(direction)), 0)
            info = "  ".join([v + " " * (max_len_list[i] - _len(v)) for i, v in enumerate(info)])
            changes.append("{elem}: {direction}{' ' * gap}({info})".format(elem=elem, direction=direction, info=info))
        else:
            changes.append("{elem}: {direction}".format(elem=elem, direction=direction))
    
    for change in changes:
        print(change)


def truncate_text(text, max_width):
    """
    根据显示宽度截断文本，添加省略号
    """
    if not isinstance(text, str):
        text = str(text)
    
    if _len(text) <= max_width:
        return text + " " * (max_width - _len(text))
    
    tmp_len = 0
    chunk_text = ""
    for char in text:
        tmp_len += _len(char)
        chunk_text += char
        if tmp_len + 3 > max_width:
            break
    
    if chunk_text != text:
        chunk_text = chunk_text + "." * max_width
    else:
        chunk_text = chunk_text + "  "
    
    right_cut = _len(chunk_text) - max_width
    return chunk_text[:-right_cut]


def print_df(df, k=None, max_width=50, **kwargs):
    """
    打印对齐的表格，支持中文和ANSI颜色代码，用下面这个符号替代spark中的打印符号："┏", "┳", "┓", "━", "┃", "┗", "┻", "┛"，"┣", "╋", "┫",
    参数:
    df: pandas DataFrame
    k: 显示的行数
    max_width: 每列最大宽度
    title: 表格标题
    """
    col_map = kwargs.pop("col_map", {})
    cut_len_map = kwargs
    if df.empty:
        print("空的DataFrame")
        return
    
    def trans_ele(x):
        if isinstance(x, list):
            return ",".join([str(i) for i in x])
        elif isinstance(x, dict):
            return ",".join([f"{k}:{v}" for k, v in x.items()])
        elif isinstance(x, float):
            if str(x).lower() == "nan":
                return "N/A"
            else:
                return str(x).split(".")[0] + "." + str(x).split(".")[1][:5]
        else:
            return str(x)
    
    df_head = df.head(k).rename(col_map, axis=1)
    for i in df_head.columns:
        df_head[i] = df_head[i].apply(lambda x: trans_ele(x).strip().replace("\t", "").replace("\n", "").replace("\r", "").replace(" ", ""))
    
    # 计算每列需要的宽度
    col_widths = {}
    for col in df_head.columns:
        # 考虑列名和所有数据的显示宽度
        widths = [_len(str(col))]  # 列名宽度
        for val in df_head[col]:
            widths.append(_len(str(val)))
        tmp_max_width = cut_len_map.get(col, max_width)
        if tmp_max_width == -1:
            tmp_max_width = 1000
        if not isinstance(tmp_max_width, int):
            raise Exception(f"额外入參为指定列宽度，格式为：col_name=col_max_len，其中value为int格式，你的输入：{col}={tmp_max_width}")
        col_widths[col] = min(max(widths), tmp_max_width)
    
    # 打印标题
    total_width = sum(col_widths.values()) + 3 * len(col_widths) + 1
    print("+" + "-" * (total_width - 2) + "+")
    
    # 打印表头
    header_line = "|"
    separator_line = "|"
    for col in df_head.columns:
        header_line += f" {truncate_text(str(col), col_widths[col]):<{col_widths[col]}} |"
        separator_line += "-" * (col_widths[col] + 2) + "|"
    print(header_line)
    print(separator_line.replace("|", "+"))
    
    # 打印数据行
    for _, row in df_head.iterrows():
        row_line = "|"
        for col in df_head.columns:
            cell_value = truncate_text(str(row[col]), col_widths[col])
            row_line += f" {cell_value} |"
        print(row_line)
    
    print("+" + "-" * (total_width - 2) + "+")


def print_df_v1(df, k=None, max_width=50, **cut_len_map):
    """
    打印对齐的表格，支持中文和ANSI颜色代码
    
    参数:
    df: pandas DataFrame
    k: 显示的行数
    max_width: 每列最大宽度
    title: 表格标题
    """
    if df.empty:
        print("空的DataFrame")
        return
    
    df_head = df.head(k)
    for i in df.columns:
        df_head[i] = df_head[i].apply(lambda x: str(x).strip().replace("\t", "").replace("\n", "").replace("\r", "").replace(" ", ""))
    
    # 计算每列需要的宽度
    col_widths = {}
    for col in df_head.columns:
        # 考虑列名和所有数据的显示宽度
        widths = [_len(str(col))]  # 列名宽度
        for val in df_head[col]:
            widths.append(_len(str(val)))
        tmp_max_width = cut_len_map.get(col, max_width)
        if tmp_max_width == -1:
            tmp_max_width = 1000
        if not isinstance(tmp_max_width, int):
            raise Exception(f"额外入參为指定列宽度，格式为：col_name=col_max_len，其中value为int格式，你的输入：{col}={tmp_max_width}")
        col_widths[col] = min(max(widths), tmp_max_width)
    
    # 打印标题
    total_width = sum(col_widths.values()) + 4 * len(col_widths) + 1
    print("+" + "-" * (total_width - 2) + "+")
    
    # 打印表头
    header_line = "|"
    separator_line = "|"
    for col in df_head.columns:
        header_line += f" {truncate_text(str(col), col_widths[col])} |"
        separator_line += "-" * (col_widths[col] + 3) + "|"
    print(header_line)
    print(separator_line.replace("|", "+"))
    
    # 打印数据行
    for _, row in df_head.iterrows():
        row_line = "|"
        for col in df_head.columns:
            cell_value = truncate_text(str(row[col]), col_widths[col])
            row_line += f" {cell_value} |"
        print(row_line)
    
    print("+" + "-" * (total_width - 2) + "+")
