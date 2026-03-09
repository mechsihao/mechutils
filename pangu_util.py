# -*- coding:utf-8 -*-
"""
pangu比较特殊，路径后面必须有/，否则都代表文件
"""

import os
import time
import random
import datetime
import subprocess
from glob import glob

pu_cmd = "pu" if glob("/bin/pu") else "/apsara/deploy/pu"


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
            print("[WARNING] Prefix match noting! pangu dir_prefix: %s, base_prefix: %s, suffixes: %s" % (dir_prefix, base_prefix, suffixes))
            return []
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


def get_file_meta_info(file):
    try:
        msg = os.popen("%s meta %s" % (pu_cmd, file)).read()
        if msg == "error: File does not exist":
            raise FileNotFoundError("File does not exist")
        msg_arr = [i for i in msg.split("\n") if i.strip()][1:]
        return {k.strip(): v.strip() for k, v in map(lambda x: x.split(": "), msg_arr) if k.strip() and v.strip()}
    except Exception as e:
        raise FileNotFoundError("[ERROR] <%s> File not found in pangu: %s, detail: %s" % (now(), file, str(e)))


def read_local_ptag(file):
    ptag_file = os.path.join(os.path.dirname(file), "." + os.path.basename(file)) + ".ptag"
    return [line for line in open(ptag_file) if line][0] if os.path.exists(ptag_file) else None


def save_local_ptag(ptag, file):
    """将etag文件存储成隐藏文件
    """
    ptag_file = os.path.join(os.path.dirname(file), "." + os.path.basename(file) + ".ptag")
    if os.path.exists(ptag_file):
        os.remove(ptag_file)
    with open(ptag_file, "w") as f:
        f.write(ptag)


def get_file_ptag(x):
    assert isinstance(x, str), "[ERROR] Unknown input: %s" % x
    info = get_file_meta_info(x)
    return '\t'.join([x, info["Length"], info["CreateTime"], info["LastModifyTime"]])


def merge_file(dir_str, out_file, pattern="*"):
    return os.system('find %s -maxdepth 1 -type f -name "%s" -print0 | sort -z | xargs -0 cat > %s' % (dir_str, pattern, out_file)) == 0


def _download_file(pangu_file, local_file, be_quiet=False):
    """下载单文件
    """
    local_dir = os.path.dirname(local_file)
    cmd = "%s get %s %s" % (pu_cmd, pangu_file, local_file)
    if be_quiet:
        cmd += " > /dev/null"
    if os.path.exists(local_dir):
        if os.system(cmd):
            raise Exception("[ERROR]Download failed! pangu path: %s" % (pangu_file))
        else:
            print("[INFO] <%s> Download success! local path: %s" % (now(), local_file))
    else:
        raise Exception("[ERROR] Local dir not found: %s" % (local_dir))


def download_file(pangu_file, local_file, read_cache=True, ignore_error=False, be_quiet=False):
    """下载总入口，会同时保存ptag，对比ptag不一致才会下载
    """
    try:
        pangu_ptag = get_file_ptag(pangu_file)
        local_ptag = read_local_ptag(local_file)
        if read_cache and os.path.exists(local_file) and pangu_ptag == local_ptag:
            print("[INFO] <%s> Remote pangu file '%s' dose not change, do not download." % (now(), pangu_file))
            return pangu_ptag, 0
        else:
            _download_file(pangu_file, local_file, be_quiet)
            save_local_ptag(pangu_ptag, local_file)
            return pangu_ptag, 1
    except Exception as e:
        if ignore_error:
            print(e)
        else:
            raise Exception(e)
        
        
def count_pu_upload_processes(pattern: str = "") -> int:
    """
    统计当前正在运行的pu put overwritten进程数量
    """
    try:
        # 使用ps命令查找pu put进程
        result = subprocess.run(
            ['ps', 'aux'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        count = 0
        lines = result.stdout.strip().split('\n')
        
        for line in lines:
            if 'pu' in line and 'put' in line and 'overwritten' in line and '-m' in line:
                if 'grep' not in line and str(os.getpid()) not in line and pattern in line:
                    count += 1
        return count
    except Exception as e:
        print(f"[WARNING] Failed to count pu processes: {e}")
        return 0


def upload_file(local_file, pangu_file, be_quiet=False):
    cmd = "%s put -m overwritten %s %s" % (pu_cmd, local_file, pangu_file)
    if be_quiet:
        cmd += " > /dev/null"
    if os.system(cmd):
        raise Exception("[ERROR] Upload failed! local path: %s" % local_file)
    else:
        print("[INFO] <%s> Upload success! pangu path: %s" % (now(), pangu_file))


def wait_for_upload_slot(pattern: str = "", max_concurrent: int = 5, check_interval: float = 1, timeout: int = 300):
    """
    等待可用的上传槽位
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        current_count = count_pu_upload_processes(pattern)
        if current_count < max_concurrent:
            print(f"[DEBUG] Process {os.getpid()}: Found available pangu put slot ({current_count}/{max_concurrent})")
            return True
        else:
            print(f"[DEBUG] Process {os.getpid()}: Waiting for pangu put slot ({current_count}/{max_concurrent})")
            time.sleep(check_interval*(1+random.randint(0, 90)/10))
    raise Exception(f"Timeout waiting for upload slot after {timeout} seconds")


def upload_file_safe(local_file, pangu_file, max_retries=3, timeout=600, be_quiet=False):
    """
    大数据量高并发上传用到，使用进程监控控制并发
    """
    # 从环境变量获取最大并发数，默认为5
    max_concurrent = os.getenv('PANGU_MAX_CONCURRENT_UPLOADS')
    if max_concurrent is None:
        print("[WARNING] Env param `PANGU_MAX_CONCURRENT_UPLOADS` not set, this param must be used while safe upload to pangu, using default value 5")
        max_concurrent = 5
    else:
        max_concurrent = int(max_concurrent)
    
    for attempt in range(max_retries):
        try:
            wait_for_upload_slot("", max_concurrent, timeout=timeout)
            cmd = [pu_cmd, "put", "-m", "overwritten", local_file, pangu_file]
            if be_quiet:
                cmd.append("> /dev/null")
            print(f"[INFO] Process {os.getpid()}: Starting upload {local_file} -> {pangu_file}")
            result = subprocess.run(cmd, timeout=timeout)
            if result.returncode == 0:
                print("[INFO] <%s> Upload success! pangu path: %s" % (now(), pangu_file))
                return
            else:
                print("[ERROR] <%s> Upload failed! local path: %s, error: %s" % (now(), local_file, result.stderr))
        except subprocess.TimeoutExpired:
            print("[WARNING] <%s> Upload timeout, retrying... (%d/%d)" % (now(), attempt + 1, max_retries))
        except subprocess.CalledProcessError as e:
            print("[ERROR] <%s> Upload failed! local path: %s, error: %s" % (now(), local_file, e.stderr))
        except Exception as e:
            print("[ERROR] <%s> Upload failed! local path: %s, error: %s" % (now(), local_file, str(e)))
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # 指数退避
    raise Exception("[ERROR] Upload failed after %d retries! local path: %s" % (max_retries, local_file))
