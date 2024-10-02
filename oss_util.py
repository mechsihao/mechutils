import os
import datetime


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
        
            
def glob_oss(file_pattern):
    """oss通配符，只支持: *, %d, ?
        - *:任意字符串
        - %d:任意整数
        - ?:任意一个字符
        - 注意，只能同时出现一种通配符，不允许混用！
    """
    stack = [i for i in ["*", "%d", "?"] if i in file_pattern]
    assert len(stack) == 1 or len(stack) == 0, "[ERROR] Do not support mutlt wildcard!, find %s" % stack
    assert any([i not in file_pattern for i in ["/*", "/%d", "/?"]]), "[ERROR] Do not support `.../*...`, `.../?...` or `.../%d...`, because oss system must have prefix!"
    if len(stack) == 0:
        res = [line.strip().split(" ")[-1] for line in os.popen("osscmd listallobject %s" % file_pattern).read().split("\n") if "oss://" in line and line.strip()]
        res = [i for i in res if i == file_pattern]
    else:
        prefix = file_pattern.split(stack[0])[0]
        suffixes = file_pattern.split(stack[0])[1:]
        
        msg = os.popen("osscmd listallobject %s" % prefix).read()
        if "Error Status:\n\n404" in msg:
            raise FileNotFoundError("[ERROR] File dose not exist! oss prefix: %s, detail: %s" % (prefix, msg))
        elif msg.startswith("object list number is: 0"):
            print("[WARNING] Prefix match noting! pattern: %s" % (prefix))
            return []
        else:
            res = list(
                    filter(
                        lambda x: x.startswith("oss://"), 
                        map(
                            lambda x: x[3], 
                            filter(
                                lambda x: len(x) == 5, 
                                map(lambda x: x.split(" "), msg.split("\n"))
                            )
                        )
                    )
                )
            res = __filter_star_mark(res, file_pattern)
            res = __filter_digitial_mark(res, file_pattern)
            res = __filter_question_mark(res, file_pattern)
            if not res:
                print("[WARNING] Suffixes match noting! suffixes: %s" % ', '.join(suffixes))
    if not res:
        print("[WARNING] Match noting! file_pattern='%s'" % file_pattern)
    return res


def get_file_meta_info(file):
    try:
        msg = os.popen("osscmd meta %s" % file).read()
        if msg.startswith("Error Headers"):
            raise FileNotFoundError(f"not found")
        msg_arr = [i for i in msg.split("\n") if i.strip()]
        return {k.strip(): v.strip() for k, v in map(lambda x: x.split(": "), msg_arr) if k.strip() and v.strip()}
    except Exception as e:
        raise FileNotFoundError("[ERROR] <%s> File not found in oss: %s, detail: %s" % (now(), file, str(e)))


def save_local_etag(etag, file):
    """将etag文件存储成隐藏文件
    """
    etag_file = os.path.join(os.path.dirname(file), "." + os.path.basename(file) + ".etag")
    if os.path.exists(etag_file):
        os.remove(etag_file)
    with open(etag_file, "w") as f:
        f.write(etag)


def read_local_etag(file):
    etag_file = os.path.join(os.path.dirname(file), "." + os.path.basename(file)) + ".etag"
    return [line for line in open(etag_file) if line][0] if os.path.exists(etag_file) else ""


def get_file_etag(x):
    assert isinstance(x, str) or isinstance(x, dict), "[ERROR] Unknown input: %s" % x
    info = get_file_meta_info(x) if isinstance(x, str) else x
    return info["etag"]


def get_file_size(x):
    """返回oss文件的大小，单位GB
    """
    assert isinstance(x, str) or isinstance(x, dict), "[ERROR] Unknown input: %s" % x
    info = get_file_meta_info(x) if isinstance(x, str) else x
    return float(info["content-length"]) / 1024 / 1024 / 1024


def get_file_size_local(x):
    """返回local文件的大小，单位GB
    """
    res = os.popen("du -a %s" % x).read().strip().split("\t")[0]
    return float(res) / 1024 / 1024
    

def download_file_single(oss_file, local_file):
    loacl_dir = os.path.dirname(local_file)
    if os.path.exists(loacl_dir):
        if os.system("osscmd get %s %s" % (oss_file, local_file)):
            raise Exception("[ERROR] Download failed! oss path: %s" % oss_file)
        else:
            print("[INFO] <%s> Download success! local path: %s\n" % (now(), local_file), end="")
    else:
        raise Exception("[ERROR] Local dir not found: %s" % loacl_dir)


def download_file_multi(oss_file, local_file, thread=5):
    """文件过大则使用这个下载
    """
    loacl_dir = os.path.dirname(local_file)
    if os.path.exists(loacl_dir):
        if os.system("osscmd multiget %s %s --thread_num=%s" % (oss_file, local_file, thread)):
            raise Exception("[ERROR] Download failed! oss path: %s" % oss_file)
        else:
            print("[INFO] <%s> Download success! local path: %s\n" % (now(), local_file), end="")
    else:
        raise Exception("[ERROR] Local dir not found: %s" % loacl_dir)


def download_file(oss_file, local_file, thr=2, read_cache=True):
    """
    下载总入口，会同时保存etag，对比etag不一致才会下载
    Args:
        :param thr: 分片下载阈值，单位GB
    """
    meta_info = get_file_meta_info(oss_file)
    oss_etag = get_file_etag(meta_info)
    local_etag = read_local_etag(local_file)
    if read_cache and oss_etag == local_etag:
        print("[INFO] <%s> Remote oss file '%s' dose not change, do not download\n" % (now(), oss_file), end="")
        return oss_etag, 0
    else:
        download_file_multi(oss_file, local_file) if get_file_size(meta_info) > thr else download_file_single(oss_file, local_file)
        save_local_etag(oss_etag, local_file)
        return oss_etag, 1


def upload_file_single(local_file, oss_file):
    if os.system("osscmd put %s %s" % (local_file, oss_file)):
        raise Exception("[ERROR] Upload failed! local path: %s" % local_file)
    else:
        print("[INFO] <%s> Upload success! oss path: %s\n" % (now(), oss_file), end="")


def upload_file_multi(local_file, oss_file, thread=5):
    """文件过大则使用这个上传
    """
    if os.system("osscmd multiupload %s %s --thread_num=%s" % (local_file, oss_file, thread)):
        raise Exception("[ERROR] Upload failed! local path: %s" % local_file)
    else:
        print("[INFO] <%s> Upload success! oss path: %s\n" % (now(), oss_file), end="")


def upload_file(local_file, oss_file, thr=2):
    """上传文件总入口
    """
    upload_file_multi(local_file, oss_file) if get_file_size_local(local_file) > thr else upload_file_single(local_file, oss_file)


def config_oss(access_id, access_key, host="oss-cn-hangzhou-zmf.aliyuncs.com"):
    if os.system("osscmd config --id=%s --key=%s --host=%s" % (access_id, access_key, host)):
        raise Exception("[ERROR] <%s> Config oss failed! access_id=%s, access_key=%s, host= %s" % (access_id, access_key, host))
    else:
        print("[INFO] <%s> Config oss success!" % now())
