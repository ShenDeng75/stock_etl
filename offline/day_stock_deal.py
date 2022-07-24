#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
from datetime import datetime

import tushare as ts
import pandas as pd
from pandas import DataFrame
from retrying import retry

from common.logger import logger
from common.properties import Date
from common.tools import Sink, Source
from offline import month_stock_dim

pd.set_option('display.width', 200)
pd.set_option('display.max_columns', None)

ts.set_token("c20ea165fe87e91c0eec2f1fb529b29e0a1ce3ee61f017dc96f64c7b")
pro = ts.pro_api()

table_name = 't_stock_deal'


# 个股历史交易[未复权](最多重试5次，每次间隔3秒)
@retry(stop_max_attempt_number=5, wait_fixed=3000)
def get_stock_deal(ts_code, start_date, end_date, adj) -> DataFrame:
    stock_df = DataFrame()
    if adj == 'none':
        stock_df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    else:
        stock_df = pro.pro_bar()
    rows = stock_df.shape[0]
    logger.info("从 pro.daily接口获取个股交易天数据 %d行" % rows)
    dt = time.strftime('%Y%m%d')
    dt_col = [dt for _ in range(rows)]
    # 获取数据的时间
    stock_df.insert(loc=0, column='input_time', value=dt_col)
    # 未复权
    adj_col = [adj for _ in range(rows)]
    stock_df.insert(loc=0, column='adj', value=adj_col)

    return stock_df


# 获取个股历史数据（默认获取昨天）
def get_history_deal(adj, start_date=Date.ystday, end_date=Date.ystday):
    ts_codes = Source.mysql_args2df(month_stock_dim.table_name, columns=['ts_code'])['ts_code'].tolist()
    res_stock_deal = DataFrame()
    days = (datetime.strptime(end_date, Date.fmt_ds) - datetime.strptime(start_date, Date.fmt_ds)).days + 1
    # 由于接口一次最多获取5000行，并且单次查询的股票数量超过1000就会失败
    # 考虑查询的时间段内有非交易日，所以实际天数是交易日的约0.3倍
    step = min(int(5000 / days * 1.3), 1000)
    for idx in range(0, len(ts_codes), step):
        str_ts_codes = ','.join(ts_codes[idx:idx + step])
        batch = get_stock_deal(str_ts_codes, start_date, end_date, adj)
        res_stock_deal = pd.concat([res_stock_deal, batch])

    return res_stock_deal


def run():
    stock = get_history_deal()
    Sink.df_to_mysql(stock, table_name)
    # print(stock)


if __name__ == "__main__":
    run()