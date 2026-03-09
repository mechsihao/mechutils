# -*- coding:utf-8 -*-
import os
import pandas as pd
import pandas._libs.lib as lib
from glob import glob
import datetime


# hdfs_cmd = "{}bin/hdfs".format(os.environ.get('HADOOP_HOME')) if 'HADOOP_HOME' in os.environ else "hdfs"
hdfs_cmd = "hdfs"


def today(fmt="%Y%m%d.%H%M"):
    return datetime.datetime.today().strftime(fmt)


def now():
    return today(fmt="%Y.%m.%d-%H:%M:%S")


def _download_file(hdfs_file, local_file, read_cache=True, merge=True):
    opt = "getmerge" if merge else "get"
    if os.path.exists(local_file) and read_cache:
        print("[WARNING] Local file {} exist, do not download.".format(local_file))
    else:
        if os.system("{} dfs -{} {} {}".format(hdfs_cmd, opt, hdfs_file, local_file)):
            raise Exception("[ERROR] Downlaod failed! hdfs file: {}".format(hdfs_file))
        else:
            print("[INFO] Download success! local file: {}".format(local_file))


def download_file_list(hdfs_root, file_list, local_root, redownload=False, merge=True):
    for file_name in file_list:
        hdfs_file = os.path.join(hdfs_root, file_name)
        local_file = os.path.join(local_root, file_name)
        download_file(hdfs_file, local_file, redownload, merge)

    
def upload_file(local_file, hdfs_file):
    if os.system("{} dfs -put -f '{}' '{}'".format(hdfs_cmd, local_file, hdfs_file)):
        raise Exception("Uplaod failed! local file: {}".format(local_file))
    else:
        print("[INFO] Uplaod success! hdfs file: {}".format(hdfs_file))


def glob_hdfs(hdfs_file):
    x = os.popen("{} dfs -ls {}*".format(hdfs_cmd, hdfs_file)).read()
    return sorted([i.split()[-1] for i in x.split("\n") if 'hdfs' in i and '_SUCCESS' not in i])


def get_file_meta_info(file):
    try:
        msg = os.popen("%s dfs -ls -h %s" % (hdfs_cmd, file)).read().strip()
        if "No such file or directory" in msg:
            raise FileNotFoundError("File does not exist")
        chmod, backup_nums, user, group, length, create_day, create_time, file_path = msg.split()
        return {"chmod": chmod, "backup_nums": backup_nums, "user": user, "group": group, "length": length, "create_day": create_day, "create_time": create_time, "file_path": file_path}
    except Exception as e:
        raise FileNotFoundError("[ERROR] <%s> File not found in pangu: %s, detail: %s" % (now(), file, str(e)))


def read_local_htag(file):
    ptag_file = os.path.join(os.path.dirname(file), "." + os.path.basename(file)) + ".htag"
    return [line for line in open(ptag_file) if line][0] if os.path.exists(ptag_file) else None


def save_local_htag(htag, file):
    """将htag文件存储成隐藏文件
    """
    htag_file = os.path.join(os.path.dirname(file), "." + os.path.basename(file) + ".htag")
    if os.path.exists(htag_file):
        os.remove(htag_file)
    with open(htag_file, "w") as f:
        f.write(htag)


def get_file_htag(x):
    assert isinstance(x, str), "[ERROR] Unknown input: %s" % x
    info = get_file_meta_info(x)
    return '\t'.join([x, info["length"], info["create_day"], info["create_time"]])


def download_file(hdfs_file, local_file, read_cache=True, merge=False, ignore_error=False):
    """下载总入口，会同时保存ptag，对比ptag不一致才会下载
    """
    try:
        pangu_ptag = get_file_htag(hdfs_file)
        local_ptag = read_local_htag(local_file)
        if read_cache and os.path.exists(local_file) and pangu_ptag == local_ptag:
            print("[INFO] <%s> Remote pangu file '%s' dose not change, do not download." % (now(), hdfs_file))
            return pangu_ptag, 0
        else:
            _download_file(hdfs_file, local_file, read_cache, merge)
            save_local_htag(pangu_ptag, local_file)
            return pangu_ptag, 1
    except Exception as e:
        if ignore_error:
            print(e)
        else:
            raise Exception(e)