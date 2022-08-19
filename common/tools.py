# 公共的处理方法
import json
import os

import pandas as pd
import pyhdfs
from kafka import KafkaProducer
from pandas import DataFrame, Series
from sqlalchemy import create_engine

from common.logger import logger
from common.properties import conf, Fields

mysql_conn_url = 'mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8' % (
    conf.mysql_username, conf.mysql_pwd, conf.mysql_host, conf.mysql_port, conf.mysql_db)
conn = create_engine(mysql_conn_url)


class Common:
    @staticmethod
    def is_trade_day(ds):
        df_trade_day = Source.mysql_args2df(Fields.trade_day_table_name, where={'cal_date': ds}, columns=['is_open'])
        if df_trade_day.shape[0] != 1:
            logger.error('交易日期获取失败')
            return -1
        is_trade_day = df_trade_day.iloc[0, 0]

        return is_trade_day


class Sink:
    MODEL_APPEND = "append"
    MODEL_REPLACE = "replace"

    @staticmethod
    def json_str_to_kafka(json_str: str, topic: str):
        kfk_prod = KafkaProducer(sasl_mechanism="PLAIN",
                                 security_protocol='SASL_PLAINTEXT',
                                 sasl_plain_username=conf.kfk_user,
                                 sasl_plain_password=conf.kfk_passwd,
                                 bootstrap_servers=conf.kfk_bt_servers, retries=3,
                                 value_serializer=lambda x: x.encode("utf-8"))
        json_data = json.loads(json_str)
        if isinstance(json_data, dict):
            kfk_prod.send(topic=topic, value=json.dumps(json_data, ensure_ascii=False))
            logger.info("写入数据到topic '%s' 1 行" % topic)
        if isinstance(json_data, list):
            for idx, msg in enumerate(json_data):
                future = kfk_prod.send(topic=topic, value=json.dumps(msg, ensure_ascii=False))
                # 调用get方法是同步方式，性能较低；不调用是异步方式，异步方式存在丢失数据的问题；
                # 如果集群不稳定，且数据量不大，最好使用同步方式；
                meta_data = future.get(10)
                logger.debug("kafka生产数据： topic %s, partition %s, offset %s"
                             % (meta_data.topic, meta_data.partition, meta_data.offset))
            logger.info("写入数据到topic '%s' %d 行" % (topic, len(json_data)))

    @staticmethod
    def df_to_mysql(df: DataFrame, table_name, if_exists=MODEL_APPEND):
        df.to_sql(name=table_name, con=conn, if_exists=if_exists, index=False)
        logger.info("插入数据到MySQL表: '%s' 行数(%d)" % (table_name, df.shape[0]))

    @staticmethod
    def df_to_hdfs(path_dir, df: DataFrame, path_col, overwrite=True, row_sep='\001', col_sep='\n'):
        '''将数据写入到path_col列对应的文件路径中'''
        client = pyhdfs.HdfsClient(hosts=conf.hdfs_hosts, user_name=conf.hdfs_user)
        dir_path = os.path.dirname(path_dir)
        if not client.exists(dir_path):
            client.mkdirs(dir_path)

        key: Series = df[path_col]
        first = df[df.columns[0]]
        rdf: DataFrame = df.drop(columns=df.columns[0], axis=1)
        # 将所有列拼接为一个字符串列
        value: Series = first.map(str).str.cat(rdf.astype(str), sep=row_sep)
        mid_df = pd.concat([key, value], axis=1, ignore_index=True)
        mid_df.columns = ['key', 'value']
        # 将同路径的合并为一行，key为路径，value为数据
        res: DataFrame = mid_df.groupby('key')['value'].apply(lambda x: x.str.cat(sep=col_sep)).reset_index()
        for item in res.values:
            path = '/'.join([path_dir, item[0][:6], item[0]]) + '.txt'
            data: str = item[1]
            client.create(path, data.encode("utf-8"), overwrite=overwrite)

            logger.info("写入 %d 行数据到文件 %s 中" % (data.count('\n') + 1, path))


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
    Sink.json_str_to_kafka('[{"name":"shendeng","age":13},{"name":"saaa","age":45}]', conf.topic_stock)
