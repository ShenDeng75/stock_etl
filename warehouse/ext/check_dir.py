"""
检查交易日数据的 hdfs结果目录是否存在
"""
import sys

import pyhdfs

from common.properties import conf
from common.tools import Common


def check(params):
    if "check_dir" not in params:
        raise KeyError("参数check_dir是必须的")
    check_dir: str = params["check_dir"]
    check_trade = params["check_trade"]

    if check_trade == "True":
        dt = check_dir.rsplit("/", 1)[1]
        if not Common.is_trade_day(dt):
            print("%s 不是交易日，无需判断" % dt)
            return 0

    client = pyhdfs.HdfsClient(hosts=conf.hdfs_hosts, user_name=conf.hdfs_user)
    if not client.exists(check_dir):
        raise FileNotFoundError("%s 路径不存在" % check_dir)

    return 0


if __name__ == "__main__":
    args = sys.argv
    params = {"check_trade": "True"}  # 默认检查最后的目录名是否为交易日
    for item in args:
        kv = item.split("=")
        params[kv[0]] = kv[1]

    check(params)
