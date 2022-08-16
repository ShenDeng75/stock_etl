#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
from datetime import datetime

import pandas as pd
import tushare as ts
from pandas import DataFrame
from retrying import retry

from common.logger import logger
from common.properties import Date, conf
from common.tools import Sink, Source, Common
from offline import month_stock_dim

pd.set_option('display.width', 200)
pd.set_option('display.max_columns', None)

ts.set_token(conf.ts_token)
pro = ts.pro_api()

table_name = 't_stock_deal'
ext_table_name = 't_ext_stock_deal'


# 个股历史交易[未复权](最多重试5次，每次间隔3秒)
@retry(stop_max_attempt_number=5, wait_fixed=3000)
def get_stock_deal(ts_code, start_date, end_date, adj) -> DataFrame:
    if adj == 'none':
        stock_df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    else:
        stock_df = ts.pro_bar(ts_code=ts_code, adj=adj, asset='E', freq='D', start_date=start_date, end_date=end_date)
    rows = stock_df.shape[0]
    logger.info("获取个股交易天粒度数据 %d行，复权类型 %s" % (rows, adj))
    dt = time.strftime('%Y%m%d')
    dt_col = [dt for _ in range(rows)]
    # 获取数据的时间
    stock_df.insert(loc=0, column='input_time', value=dt_col)
    # 复权类型
    adj_col = [adj for _ in range(rows)]
    stock_df.insert(loc=0, column='adj', value=adj_col)

    return stock_df


# 获取个股历史数据（默认获取昨天）
def get_history_deal(adj='none', ts_codes: list = None, start_date=Date.ystday, end_date=Date.ystday):
    if ts_codes is None:
        ts_codes = Source.mysql_args2df(month_stock_dim.table_name, columns=['ts_code'])['ts_code'].tolist()
        days = (datetime.strptime(end_date, Date.fmt_ds) - datetime.strptime(start_date, Date.fmt_ds)).days + 1
        step = min(int(5000 / days * 1.3), 1000)
    else:
        step = len(ts_codes)

    res_stock_deal = DataFrame()
    # 由于接口一次最多获取5000行，并且单次查询的股票数量超过1000就会失败，所以要分批查询
    # 考虑查询的时间段内有非交易日，所以实际天数是交易日的约0.3倍
    for idx in range(0, len(ts_codes), step):
        str_ts_codes = ','.join(ts_codes[idx:idx + step])
        batch = get_stock_deal(str_ts_codes, start_date, end_date, adj)
        # 这里要注意ignore_index参数，默认为False(表示保留原DF的index)，True(表示重新编号)
        # 例如两个行数为1000的DF通过concat合并后，总行数为2000，但是index还是原来的0~999
        # ignore_index为False是，通过df.loc[1001]时会报错，因为index中是0~999(分别有2个)
        res_stock_deal = pd.concat([res_stock_deal, batch], ignore_index=True)

    return res_stock_deal


def run():
    stock = get_history_deal()
    Sink.df_to_mysql(stock, table_name)
    # save_path = '/'.join([conf.hdfs_base_path, ext_table_name])
    # Sink.df_to_hdfs(save_path, stock, 'trade_date')


def execute():
    ds = Date.ystday
    if not Common.is_trade_day(ds):
        logger.info('%s 不是交易日' % ds)
        return 0

    run()


if __name__ == "__main__":
    execute()
