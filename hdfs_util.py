import os
import pandas as pd
import pandas._libs.lib as lib


# hdfs_cmd = "{}bin/hdfs".format(os.environ.get('HADOOP_HOME')) if 'HADOOP_HOME' in os.environ else "hdfs"
hdfs_cmd = "hdfs"


def download_file(hdfs_file, local_file, redownload=False, merge=True):
    opt = "getmerge" if merge else "get"
    if os.path.exists(local_file) and not redownload:
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


def read_csv(
    path: str, 
    sep=",", 
    header="infer", 
    names=None, 
    usecols=None, 
    dtype=None, 
    skiprows=None, 
    nrows=None, 
    encoding=None, 
    error_bad_lines=None, 
    cache_dir="/UserData/data/.cache",
    redownload=False
):
    base_name = os.path.basename(path)
    cache_file = os.path.join(cache_dir, base_name)
    if path.startswith("hdfs://"):
        download_file(path, cache_file, redownload)
        path = cache_file
    return pd.read_csv(
        path, sep=sep, header=header, names=names, usecols=usecols, dtype=dtype, 
        skiprows=skiprows, nrows=nrows, encoding=encoding, error_bad_lines=error_bad_lines
    )
