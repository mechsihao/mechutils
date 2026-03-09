# -*- coding:utf-8 -*-
import os
import datetime


config1 = {
    "id": "", 
    "key": "",
    "host": ""
}


auth_map = {
    "mechlsh": config1
}


def get_auth_str(oss_path):
    assert oss_path.startswith("oss://"), "[ERROR] Input path is not prefix of `oss://`, got {}".format(oss_path)
    bucket = oss_path[len("oss://"):].split("/")[0]
    if bucket in auth_map:
        auth_str = "--id {id} --key {key} --host='{host}'".format(**auth_map[bucket])
        return auth_str
    else:
        raise Exception("[ERROR] {} dost not find in conifg, please config it!".format(bucket))


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
    AUTH_STRING = get_auth_str(file_pattern)
    stack = [i for i in ["*", "%d", "?"] if i in file_pattern]
    assert len(stack) == 1 or len(stack) == 0, "[ERROR] Do not support mutlt wildcard!, find %s" % stack
    assert any([i not in file_pattern for i in ["/*", "/%d", "/?"]]), "[ERROR] Do not support `.../*...`, `.../?...` or `.../%d...`, because oss system must have prefix!"
    if len(stack) == 0:
        res = [line.strip().split(" ")[-1] for line in os.popen("osscmd listallobject '%s' %s" % (file_pattern, AUTH_STRING)).read().split("\n") if "oss://" in line and line.strip()]
        res = [i for i in res if i == file_pattern]
    else:
        tag = stack[0]
        prefix = file_pattern.split(tag)[0]
        file_root = os.path.dirname(prefix)
        suffixes = file_pattern.split(tag)[1:]
        
        # msg = os.popen("osscmd listallobject '%s' %s" % (prefix, AUTH_STRING)).read()
        msg = os.popen("osscmd ls '%s' %s" % (prefix, AUTH_STRING)).read()
        if "Error Status:\n\n404" in msg or 'The specified bucket does not exist' in msg:
            raise FileNotFoundError(
                "[ERROR] Bucket dose not exist! oss prefix: %s, detail: %s"
                % (prefix, msg)
            )
        elif msg.startswith("object list number is: 0"):
            print("[WARNING] Prefix match noting! pattern: %s" % (prefix))
            return []
        else:
            res = [i.split()[-2] for i in msg.strip().split("\n") if 'oss://' in i and i.split()[-2].startswith(file_root + "/")]
            res = [i for i in res if i not in [prefix, prefix+"/"]]
            # res = list(
            #     filter(
            #         lambda x: x.startswith("oss://") and x.startswith(file_root + "/"),
            #         map(
            #             lambda x: x[3],
            #             filter(
            #                 lambda x: len(x) == 5,
            #                 map(lambda x: x.split(" "), msg.split("\n")),
            #             ),
            #         ),
            #     )
            # )

            res = __filter_star_mark(res, file_pattern)
            res = __filter_digitial_mark(res, file_pattern)
            res = __filter_question_mark(res, file_pattern)
            if not res:
                print("[WARNING] Suffixes match noting! suffixes: %s" % ', '.join(suffixes))
    if not res:
        print("[WARNING] Match noting! file_pattern='%s'" % file_pattern)
    return res


def get_file_meta_info(file):
    AUTH_STRING = get_auth_str(file)
    try:
        msg = os.popen("osscmd meta '%s' %s" % (file, AUTH_STRING)).read()
        if msg.startswith("Error Headers"):
            raise FileNotFoundError("File does not exist")
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
    return [line for line in open(etag_file) if line][0] if os.path.exists(etag_file) else None


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
    

def download_file_single(oss_file, local_file, be_quiet=False):
    AUTH_STRING = get_auth_str(oss_file)
    local_dir = os.path.dirname(local_file)
    cmd = "osscmd get '%s' '%s' %s" % (oss_file, local_file, AUTH_STRING)
    if be_quiet:
        cmd += " > /dev/null"
    if os.path.exists(local_dir):
        if os.system(cmd):
            raise Exception("[ERROR] Download failed! oss path: %s" % oss_file)
        else:
            print("[INFO] <%s> Download success! local path: %s" % (now(), local_file))
    else:
        raise Exception("[ERROR] Local dir not found: %s" % local_dir)


def download_file_multi(oss_file, local_file, thread=5, be_quiet=False):
    """文件过大则使用这个下载
    """
    AUTH_STRING = get_auth_str(oss_file)
    local_dir = os.path.dirname(local_file)
    cmd = "osscmd multiget '%s' '%s' --thread_num=%s %s" % (oss_file, local_file, thread, AUTH_STRING)
    if be_quiet:
        cmd += " > /dev/null"
    if os.path.exists(local_dir):
        if os.system(cmd):
            raise Exception("[ERROR] Download failed! oss path: %s" % oss_file)
        else:
            print("[INFO] <%s> Download success! local path: %s" % (now(), local_file))
    else:
        raise Exception("[ERROR] Local dir not found: %s" % local_dir)


def download_file(oss_file, local_file, thr=2, read_cache=True, ignore_error=False, be_quiet=False, multi_download_thread_num=5):
    """
    下载总入口，会同时保存etag，对比etag不一致才会下载
    Args:
        :param thr: 分片下载阈值，单位GB
    """
    try:
        meta_info = get_file_meta_info(oss_file)
        oss_etag = get_file_etag(meta_info)
        local_etag = read_local_etag(local_file)
        if read_cache and os.path.exists(local_file) and oss_etag == local_etag:
            print("[INFO] <%s> Remote oss file '%s' dose not change, do not download." % (now(), oss_file))
            return oss_etag, 0
        else:
            if get_file_size(meta_info) > thr:
                download_file_multi(oss_file, local_file, multi_download_thread_num, be_quiet)
            else:
                download_file_single(oss_file, local_file, be_quiet)
            save_local_etag(oss_etag, local_file)
            return oss_etag, 1
    except Exception as e:
        if ignore_error:
            print(e)
        else:
            raise Exception(e)


def upload_file_single(local_file, oss_file, be_quiet=False):
    AUTH_STRING = get_auth_str(oss_file)
    cmd = "osscmd put '%s' '%s' %s" % (local_file, oss_file, AUTH_STRING)
    if be_quiet:
        cmd += " > /dev/null"
    if os.system(cmd):
        raise Exception("[ERROR] Upload failed! local path: %s" % local_file)
    else:
        print("[INFO] <%s> Upload success! oss path: %s" % (now(), oss_file))


def upload_file_multi(local_file, oss_file, thread=5, be_quiet=False):
    """文件过大则使用这个上传
    """
    AUTH_STRING = get_auth_str(oss_file)
    cmd = "osscmd multiupload '%s' '%s' --thread_num=%s %s" % (local_file, oss_file, thread, AUTH_STRING)
    if be_quiet:
        cmd += " > /dev/null"
    if os.system(cmd):
        raise Exception("[ERROR] Upload failed! local path: %s" % local_file)
    else:
        print("[INFO] <%s> Upload success! oss path: %s" % (now(), oss_file))


def upload_file(local_file, oss_file, thr=5, be_quiet=False):
    """上传文件总入口
    """
    upload_file_multi(local_file, oss_file, thread=5, be_quiet=be_quiet) if get_file_size_local(local_file) > thr else upload_file_single(local_file, oss_file, be_quiet)


def upload_dir(local_dir, oss_dir, be_quiet=False):
    """上传文件总入口
    """
    AUTH_STRING = get_auth_str(oss_dir)
    cmd = "osscmd uploadfromdir '%s' '%s' %s" % (local_dir, oss_dir, AUTH_STRING)
    if be_quiet:
        cmd += " > /dev/null"
    if os.system(cmd):
        raise Exception("[ERROR] Upload dir failed! local dir: %s" % local_dir)
    else:
        print("[INFO] <%s> Upload dir success! oss dir: %s" % (now(), oss_dir))


def copy_oss(oss_path1, oss_path2):
    """复制文件总入口
    """
    if oss_path1.endswith("/") and oss_path2.endswith("/"):
        opt = "copybucket"
    elif not oss_path1.endswith("/") and not oss_path2.endswith("/"):
        opt = "copy"
    else:
        raise Exception("[ERROR] Copy source file and target file must be same type.")

    AUTH_STRING = get_auth_str(oss_path1)
    if os.system("osscmd %s '%s' '%s' %s" % (opt, oss_path1, oss_path2, AUTH_STRING)):
        raise Exception("[ERROR] Copy file/dir failed! %s -> %s" % (oss_path1, oss_path2))
    else:
        print("[INFO] <%s> Copy file/dir success! %s -> %s" % (now(), oss_path1, oss_path2))
