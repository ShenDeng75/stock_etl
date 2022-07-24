# 配置信息
from common.encrypt import decrypt


class Config:
    # mysql配置
    mysql_host = 'localhost'
    mysql_port = 3306
    mysql_username = 'sink'
    mysql_pwd = None
    mysql_db = 'stock'
    # kafka配置
    kfk_bt_servers = ['master:9092', 'slave1:9092', 'slave2:9092']
    topic_stock = 'qiyue'
    kfk_gid_stock = 'gid_stock'


# 开发环境
class ConfigDev(Config):
    mysql_pwd = decrypt('c2hlbmRlbmc3NQ==')


# 生产环境
class ConfigPro(Config):
    mysql_host = '192.168.14.16'
    mysql_pwd = decrypt('c2hlbmRlbmc3NQ==')
    topic_stock = 'tc_stock'
