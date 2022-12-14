# 配置信息
from common.encrypt import decrypt


class Config:
    # 公共配置
    log_dir = None
    ts_token = decrypt(
        "Y2QxYzZiMTcwNDEyYzFlYjhhM2U4NmM1OGNmNzJjN2JhOTM3ZGQ0N2RmZWFiZGFlNGU3Y2YyZjliOTUyNGE1NDMzNzQ1Yzk3YTlmOTk2NTk1MmExOWQzOWQ2YzFkMWNjZmE5MzAyMDA1Mzg4ODY5MGE2MmYyMzRkYzc5MzNhNmM=")
    # mysql配置
    mysql_host = 'localhost'
    mysql_port = 3306
    mysql_username = 'sink'
    mysql_pwd = None
    mysql_db = 'stock'
    # kafka配置
    kfk_bt_servers = ['cloud:9092']
    topic_stock = 'qiyue'
    kfk_gid_stock = 'gid_stock'
    kfk_user = decrypt('ODQ0MDAxNmYzN2U5YTcxZWMwNjdkZDg2ZTZhMmI0OTg=')
    kfk_passwd = decrypt('Yjk4OGRjYzgzYjcwZWU1MGJmYTQxZjYzZDUwMzliYWI=')
    # hdfs配置
    hdfs_hosts = 'master,9000'
    hdfs_user = 'py_user'
    hdfs_base_path = '/stock/EXT'


# 开发环境
class ConfigDev(Config):
    log_dir = r'D:\Code\PythonCode\StockData\logs\stock.log'
    mysql_pwd = decrypt('Yjk4OGRjYzgzYjcwZWU1MGJmYTQxZjYzZDUwMzliYWI=')


# 生产环境
class ConfigPro(Config):
    log_dir = r'/opt/code/stock_etl/log/stock.log'
    mysql_host = 'cloud'
    mysql_pwd = decrypt('ZWM4ZGIyZTExMWVmNzZjZTQyYjcyYTg2YzQ1OTE4ZGM=')
    topic_stock = 'tc_stock'
