# 公共的处理方法
import json
import pandas as pd
from kafka import KafkaProducer
from pandas import DataFrame
from sqlalchemy import create_engine

from common import properties
from common.logger import logger

conf = properties.Config.sink_config()
mysql_conn_url = 'mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8' % (
    conf.mysql_username, conf.mysql_pwd, conf.mysql_host, conf.mysql_port, conf.mysql_db)
conn = create_engine(mysql_conn_url)


class Sink:
    kfk_prod = KafkaProducer(bootstrap_servers=conf.kfk_bt_servers, value_serializer=lambda x: x.encode("utf-8"))

    @staticmethod
    def json_str_to_kafka(json_str: str, topic: str):
        json_data = json.loads(json_str)
        if isinstance(json_data, dict):
            Sink.kfk_prod.send(topic=topic, value=json.dumps(json_data))
            logger.info("写入数据到topic '%s' 1 行" % topic)
        if isinstance(json_data, list):
            for msg in json_data:
                Sink.kfk_prod.send(topic=topic, value=json.dumps(msg))
            logger.info("写入数据到topic '%s' %d 行" % (topic, len(json_data)))

    @staticmethod
    def df_to_mysql(df: DataFrame, table_name):
        df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
        logger.info("插入数据到MySQL表: '%s' 行数(%d)" % (table_name, df.shape[0]))


class Source:
    @staticmethod
    def mysql_args2df(table_name, where: dict = None, columns: list = None):
        cols = '*' if columns is None else ', '.join(columns)
        base_sql = "select %s from %s" % (cols, table_name)
        if where is None:
            return Source.mysql_sql2df(base_sql, None)
        else:
            fil = ' and '.join(map(lambda x: x + "=%s", where.keys()))
            res_sql = base_sql + " where " + fil
            params = where.values()
            return Source.mysql_sql2df(res_sql, params)

    @staticmethod
    def mysql_sql2df(sql, params) -> DataFrame:
        '''防止SQL注入'''
        df = pd.read_sql(sql, con=conn, params=params)
        exe_sql = sql if params is None else sql % tuple(params)
        logger.info("执行sql '%s', 返回行数(%d)" % (exe_sql, df.shape[0]))
        return df


if __name__ == "__main__":
    print(Source.mysql_args2df('t_trade_day', where={'cal_date': '20220715', 'is_open': 1},
                               columns=['cal_date', 'is_open']))
