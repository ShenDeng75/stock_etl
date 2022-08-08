# 配置信息
from common.encrypt import decrypt


class Config:
    # 公共配置
    log_dir = None
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
    # hdfs配置
    hdfs_hosts = 'master,9000'
    hdfs_user = 'py_user'
    hdfs_base_path = '/stock/EXT'


# 开发环境
class ConfigDev(Config):
    log_dir = r'D:\Code\PythonCode\StockData\logs\stock.log'
    mysql_pwd = decrypt('c2hlbmRlbmc3NQ==')


# 生产环境
class ConfigPro(Config):
    log_dir = r'/opt/code/stock_etl/log/stock.log'
    mysql_host = 'cloud'
    mysql_pwd = decrypt('U2hlbmRlbmc3NTs=')
    topic_stock = 'tc_stock'
