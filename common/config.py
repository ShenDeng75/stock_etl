# 配置信息
from common.encrypt import decrypt


class Config:
    mysql_host = 'localhost'
    mysql_port = 3306
    mysql_username = 'sink'
    mysql_pwd = None
    mysql_db = 'stock'


# 开发环境
class ConfigDev(Config):
    mysql_pwd = decrypt('c2hlbmRlbmc3NQ==')


# 生产环境
class ConfigPro(Config):
    mysql_host = ''
    mysql_pwd = decrypt('')
