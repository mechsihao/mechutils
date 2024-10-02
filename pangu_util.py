"""
pangu比较特殊，路径后面必须有/，否则都代表文件
"""

import os
import tqdm
import datetime
from .multi_processor_util import parall_fun
from .print_util import print_paths

pu_cmd = "pu"


def today(fmt="%Y%m%d.%H%M"):
    return datetime.datetime.today().strftime(fmt)


def now():
    return today(fmt="%Y.%m.%d-%H:%M:%S")


def __filter_star_mark(x, file_pattern):
    if "*" not in file_pattern:
        return x
    suffixes = file_pattern.split("*")
    def find_marked_data(i):
        for suffix in suffixes:
            if not suffix:
                suffix_id = len(i)
            else:
                suffix_id = i.find(suffix)
                if suffix_id == -1:
                    return False
            i = i[suffix_id + len(suffix):]
            if not i:
                return True
        return True
    return list(filter(find_marked_data, x))


def __filter_digitial_mark(x, file_pattern):
    if "%d" not in file_pattern:
        return x
    suffixes = file_pattern.split("%d")
    def find_marked_data(i):
        is_first = True
        for suffix in suffixes:
            if not suffix:
                suffix_id = len(i)
            else:
                suffix_id = i.find(suffix)
            if suffix_id == -1:
                return False
            if not is_first:
                if not i[: suffix_id].isdigit(): 
                    return False
            else:
                is_first = False
            i = i[suffix_id + len(suffix):]
            if not i:
                return True
        return True
    return list(filter(find_marked_data, x))


def __filter_question_mark(x, file_pattern):
    if "?" not in file_pattern:
        return x
    suffixes = file_pattern.split("?")
    def find_marked_data(i):
        is_first = True
        for suffix in suffixes:
            if not suffix:
                suffix_id = len(i)
            else:
                suffix_id = i.find(suffix)
            if suffix_id == -1:
                return False
            if not is_first:
                if suffix_id != 1: 
                    return False
            else:
                is_first = False
            i = i[suffix_id + len(suffix):]
            if not i:
                return True
        return True
    return list(filter(find_marked_data, x))


def file_exist(path):
    if path.endswith("/") and not os.system("{} dirmeta {}".format(pu_cmd, path)):
        return 2
    elif not os.system("{} meta {}".format(pu_cmd, path)):
        return 1
    else:
        return 0

   
def glob_pangu(file_pattern):
    """pangu通配符，只支持: *, %d, ?
        - *:任意字符串
        - %d:任意整数
        - ?:任意一个字符
        - 注意，只能同时出现一种通配符，不允许混用！
    """
    stack = [i for i in ["*", "%d", "?"] if i in file_pattern]
    assert len(stack) == 1 or len(stack) == 0, "[ERROR] Do not support mutlt wildcard!, find %s" % stack
    if len(stack) == 0:
        flag = file_exist(file_pattern)
        if flag == 2:
            res = [os.path.join(file_pattern, line.strip()) for line in os.popen("%s ls %s" % (pu_cmd, file_pattern)).read().split("\n") if line.strip()]
        elif flag == 1:
            res = [file_pattern]
        else:
            res = []
    else:
        prefix = file_pattern.split(stack[0])[0]
        dir_prefix = os.path.dirname(prefix) + "/"
        base_prefix = os.path.basename(prefix)
        suffixes = file_pattern.split(stack[0])[1:]
        
        msg = os.popen("%s ls %s" % (pu_cmd, dir_prefix)).read()
        if not msg.strip():
            raise FileNotFoundError("[ERROR] File dose not exist! pangu dir_prefix: %s, base_prefix: %s, suffixes: %s" % (dir_prefix, base_prefix, suffixes))
        else:
            path_list = [os.path.join(dir_prefix, i) for i in msg.split() if i.strip() and i.strip().startswith(base_prefix)]
            if not path_list:
                print("[WARNING] Dir prefix match noting! dir_prefix: %s" % (dir_prefix))
                return []
            else:
                res = list(filter(lambda x: x.startswith("pangu://"), path_list))
                res = __filter_star_mark(res, file_pattern)
                res = __filter_digitial_mark(res, file_pattern)
                res = __filter_question_mark(res, file_pattern)
                if not res:
                    print("[WARNING] Suffixes match noting! suffixes: %s" % suffixes)
    if not res:
        print("[WARNING] Match noting! file_pattern='%s'" % file_pattern)
    return res


def merge_file(dir_str, out_file, pattern="*"):
    return os.system('find %s -maxdepth 1 -type f -name "%s" -print0 | sort -z | xargs -0 cat > %s' % (dir_str, pattern, out_file)) == 0


def download_file(pangu_file, local_file):
    """下载单文件
    """
    loacl_dir = os.path.dirname(local_file)
    if os.path.exists(loacl_dir):
        if os.system("%s get %s %s" % (pu_cmd, pangu_file, local_file)):
            raise Exception("[ERROR] Download failed! pangu path: %s" % pangu_file)
        else:
            print("[INFO] <%s> Download success! local path: %s" % (now(), local_file))
    else:
        raise Exception("[ERROR] Local dir not found: %s" % loacl_dir)


def download_dir(pangu_dir, local_file, thread=256, merge=True):
    """目录文件需要多线程加速下载，支持通配符，pangu_dir里面通配的是dir，会把所有dir下的文件都保存在一个本地local文件中
    """
    loacl_dir = os.path.dirname(local_file)
    loacl_base = os.path.basename(local_file)
    if os.path.exists(loacl_dir):
        tmp_dir = os.path.join(loacl_dir, ".%s" % loacl_base)
        os.makedirs(tmp_dir, exist_ok=True)
        pangu_dirs = [i for i in glob_pangu(pangu_dir) if i.endswith("/")]
        if pangu_dirs:
            print("[INFO] <%s> Matching dirs:" % now())
            print_paths(pangu_dirs)
        else:
            raise Exception("[ERROR] <%s> Match nothing!" % now())
        path_list = []
        for tmp_pdir in pangu_dirs:
            path_list.extend([i for i in glob_pangu(tmp_pdir) if not i.endswith("/")])
        assert path_list, "Match nothing file for %s" % pangu_dir
        input_args = [[i, os.path.join(tmp_dir, ".".join(i.split("/")[-2:]))] for i in path_list]
        download_file_fun = lambda x: [download_file(i[0], i[1]) for i in tqdm.tqdm(x)]
        parall_fun(download_file_fun, input_args, min(thread, len(input_args)), fun_type="data")
        if merge and merge_file(tmp_dir, local_file):
            print("[INFO] <%s> Download and merge success! local path: %s" % (now(), local_file))
        else:
            os.system("mv %s %s" % (tmp_dir, local_file))
            print("[INFO] <%s> Download success! not merge or merge failed, local dir: %s" % (now(), local_file))
    else:
        raise Exception("[ERROR] Local dir not found: %s" % loacl_dir)


def upload_file(local_file, pangu_file):
    if os.system("%s put -m overwritten %s %s" % (pu_cmd, local_file, pangu_file)):
        raise Exception("[ERROR] Upload failed! local path: %s" % local_file)
    else:
        print("[INFO] <%s> Upload success! pangu path: %s" % (now(), pangu_file))
