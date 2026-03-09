import json
import unicodedata
from json import JSONDecodeError
import re
import pandas as pd
import numpy as np


def get_json(string, bracket = '{}'):
    """
    需要两步解析:
        - 第一步解析成json
        - 第二步解析成json k：v，k为我们要求的key，v为 0 or 1 的形式
    Args:
        bracket 解析的括号格式
    """
    stack = []
    
    contain_json = []
    for i in range(len(string)):
        if string[i] == bracket[0]:
            stack.append(i)
        elif string[i] == bracket[1] and stack:
            last_barce_id = stack.pop()
            if not stack:
                contain_json.append(string[last_barce_id: i + 1])
            else:
                pass
        else:
            continue
    if len(stack)==1 and stack[0] == 0:
        # 当且仅当只有一个json串，且仅缺失左侧一个花括号，才修复
        try:
            contain_json.append(json.dumps(json.loads(string + bracket[1]), ensure_ascii=False))
            stack = []
        except:
            pass
    return contain_json, stack


def clean_json_str(s):
    """
    将json字符串中的一些基本错误改掉：
        1.将json中的所有中文标点符号全部替换为英文
        2.将json’{‘到第一个开头的所有字符串都去掉。将“}“到最后一个字符串之间的所有标点符号都去掉
    """
    # 将所有中文标点转换为英文标点
    s = unicodedata.normalize('NFKC', s).replace("。", "").replace("【", "").replace("】", "")
    # 将首位的{和第一个字符串之间的空格换行去掉，并且给末尾没有标点符号的增加逗号
    s = "}".join([i.strip() for i in "{".join([i.strip() for i in s.split("{")]).split("}")]).replace('\"\n', '\",\n').replace("\'\n", "\',\n")
    # 将json’{‘到第一个开头的所有字符串都去掉。将“}“到最后一个字符串之间的所有标点符号都去掉
    s = "}".join([i[:-1] if i and i[-1] in ",，.。" else i for i in "".join(s.split()).split("}")])
    # 将每个key后面回答的//注释去掉
    s = "{" + "\n".join([line.split("//")[0].strip() for line in s.strip()[1:-1].split("\n")]) + "}"
    return s


def parse_json_list(json_list):
    res = []
    for json_str in json_list:
        try:
            res.append(json.loads(json_str))
        except:
            res.append({})
    return res


def is_json(json_str):
    try:
        json.loads(json_str)
        return True
    except:
        return False


def min_edit_distance(a, b):
    """用最小编辑距离将标准list中的key映射
    """
    dp = [[0 for i in range(len(b) + 1)] for j in range(len(a) + 1)]
    for i in range(len(a) + 1):
        dp[i][0] = i
    for j in range(len(b) + 1):
        dp[0][j] = j
    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            if a[i - 1] == b[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + 1)
    return dp[-1][-1]


def find_most_confidence_id(d, model_order):
    """重复打标按照model_order顺序来保留
    """
    r = {}
    for k, v in d.items():
        if v not in r:
            r[v] = [k]
        else:
            r[v].append(k)
    return sorted(r.items(), key=lambda x: model_order.index(x[0]))[0][1]


def parse_json_first(x: str):
    """将response中的json串解析出来，仅支持解析出一个，如果存在多个，将会解析出key最多的一个
    """
    res_list, error_msg = get_json(x)
    try:
        if len(res_list) == 0 and len(error_msg) == 0:
            return {"succ": False, "json": {}, "error_msg": "nullkey", "locate": []}
        elif len(res_list) == 1 and len(error_msg) != 0:
            json_str = res_list[0]
            start_id = x.find(json_str)
            return {"succ": True, "json": json.loads(json_str), "error_msg": "a bad json str after a good json str", "locate": [start_id, start_id + len(json_str)]}
        elif len(error_msg) != 0:
            return {"succ": False, "json": {}, "error_msg": "failed", "locate": []}
        else:
            if len(res_list) == 1:
                json_str = res_list[0]
                start_id = x.find(json_str)
                return {"succ": True, "json": json.loads(json_str), "error_msg": "", "locate": [start_id, start_id + len(json_str)]}
            else:
                tmp_json_list = [json.loads(res) for res in res_list]
                tmp_json = sorted(tmp_json_list, key=lambda x: len(x.keys()))[-1]
                tmp_json_str = sorted(res_list, key=lambda x: len(json.loads(x).keys()))[-1]
                start_id = x.find(tmp_json_str)
                return {"succ": True, "json": tmp_json, "error_msg": "multi", "locate": [start_id, start_id + len(tmp_json_str)]}      
    except JSONDecodeError:
        try:
            tmp_json_list = [json.loads(clean_json_str(res)) for res in res_list]
            tmp_json = sorted(tmp_json_list, key=lambda x: len(x.keys()))[-1]
            tmp_json_str = sorted(res_list, key=lambda x: len(json.loads(x).keys()))[-1]
            start_id = x.find(tmp_json_str)
            return {"succ": True, "json": tmp_json, "error_msg": "clean", "locate": [start_id, start_id + len(tmp_json_str)]}
        except:
            return {"succ": False, "json": {i: d for i, d in enumerate(res_list)}, "error_msg": "except", "locate": []}
    except:
        return {"succ": False, "json": {i: d for i, d in enumerate(res_list)}, "error_msg": "except", "locate": []}

    
def parse_json_second(x: dict, standard_keys: list):
    """第二步解析，解析成json k：v，k为我们要求的key，假如有key没有对上，则用编辑最小距离替代
    """
    err_msg = []
    
    new_res = {}
    for k, v in x.items():
        most_likly_keys = standard_keys[np.argmin([min_edit_distance(k, s) for s in standard_keys])]
        new_res[most_likly_keys] = v
        
        if most_likly_keys != k:
            err_msg.append("key错误, key='{}', most_likly_keys='{}'".format(k, most_likly_keys))
    
    for k in standard_keys:
        if k not in new_res:
            new_res[k] = 0
            err_msg.append("key缺失, key='{}'".format(k))
    
    return new_res, ";".join(err_msg)


def parse_response_json(x, standard_keys: list = None):
    """解析response的总入口
    注意事项：
        - 1.该解析方法仅能在一个response 解析出一个json字符串
        - 2.一旦传入standard_keys，则会按照最小编辑距离从standard_keys中替换key，最终输出的dict，标准的key为standard_keys
        - 3.该接口会修复大模型输出中json括号的一些问题

    Arguments:
        x {[str]} -- [大模型response]
        standard_keys {[list[str]]} -- [可选，标准输出的key列表，一旦提供，则用最小编辑距离的方法来修正key，因此需要key之间的差异比较大才可以少出错]

    Returns:
        [dict] -- [keys为："succ"，"dict"，"error_msg"，"locate"，"repair_dict"]
            eg: {"succ": False, "json": json.loads(json_str), "error_msg": "succ", "locate": [start_id, start_id + len(json_str)]}，其中：
            - succ为是否解析成功
            - dict为第一步输出的dict
            - error_msg为报错信息
            - locate为定位到的json在原文中的位置[start_id, end_id]
            - repair_dict为编辑距离修正后的dict
    """
    res = parse_json_first(x)
    
    if not res["succ"]:
        return res
    elif res["succ"] and standard_keys:
        repair_dict, err_msg = parse_json_second(res["json"], standard_keys)
        res["json"] = repair_dict
        res["error_msg"] += "\nstage2 err msg: {}".format(err_msg)
    return res


def parse_mech_json(x, main_keys=None, aux_tags=None):
    def parse_json(res, x, main_keys, aux_tags):
        for tag in main_keys:
            if f"<{tag}>" in x and f"</{tag}>" in x:
                tmp_res = x[x.find(f"<{tag}>")+len(f"<{tag}>"):x.find(f"</{tag}>")].strip()
            elif f"<{tag}>" in x:
                tmp_res = x[x.find(f"<{tag}>")+len(f"<{tag}>"):].strip().strip("<>").strip()
            else:
                print(f"[ERROR] Failed parse main key, detail: not found <{tag}>...</{tag}>")
                res[tag] = ""
                res["status"] = "failed"
                return res
            res[tag] = tmp_res
        if aux_tags:
            for tag in aux_tags:
                if f"<{tag}>" in x and f"</{tag}>" in x:
                    tmp_res = x[x.find(f"<{tag}>")+len(f"<{tag}>"):x.find(f"</{tag}>")].strip()
                else:
                    print(f"[WARNING] Failed parse aux key, detail: not found <{tag}>...</{tag}>")
                    tmp_res = ""
                res[tag] = tmp_res    
    
    def extract_parts(s):
        pattern_open_tag = r'<([^>]*)>'
        open_tags = re.findall(pattern_open_tag, s)
        pattern_close_tag = r'<\/([^\>]*)>'
        close_tags = re.findall(pattern_close_tag, s)
        return open_tags, close_tags

    x = x.strip().strip("`").strip()
    res = {"status": "succ"}
    if main_keys:
        parse_json(res, x, main_keys, aux_tags)
    else:
        open_tags, close_tags = extract_parts(x)
        valid_tags = [i for i in open_tags if i in close_tags]
        parse_json(res, x, valid_tags, None)
    return res


if __name__ == "__main__":
    a = """
    <shot>asdasdasd</shot>
    <answer>asdasdasd</answer>
    """
    res = parse_mech_json(a)
    print(res)