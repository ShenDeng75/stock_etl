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

ts.set_token("c20ea165fe87e91c0eec2f1fb529b29e0a1ce3ee61f017dc96f64c7b")
pro = ts.pro_api()

table_name = 't_stock_basic'
ext_table_name = 't_ext_stock_basic'


# 个股历史基本面(最多重试5次，每次间隔13秒)，该接口每分钟最多访问5次，每天最多50次
@retry(stop_max_attempt_number=3, wait_fixed=13000)
def get_stock_basic(ts_code, start_date, end_date) -> DataFrame:
    stock_df = pro.bak_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    rows = stock_df.shape[0]
    logger.info("获取个股天粒度基本面数据 %d行" % rows)
    dt = time.strftime('%Y%m%d')
    dt_col = [dt for _ in range(rows)]
    # 获取数据的时间
    stock_df.insert(loc=0, column='input_time', value=dt_col)

    return stock_df


# 获取个股历史基本面（默认获取昨天）
def get_history_basic(ts_codes: list = None, start_date=Date.ystday, end_date=Date.ystday):
    if ts_codes is None:
        ts_codes = Source.mysql_args2df(month_stock_dim.table_name, columns=['ts_code'])['ts_code'].tolist()
        days = (datetime.strptime(end_date, Date.fmt_ds) - datetime.strptime(start_date, Date.fmt_ds)).days + 1
        # 由于接口一次最多获取5000行，并且单次查询的股票数量超过1000就会失败，所以要分批查询
        # 考虑查询的时间段内有非交易日，所以实际天数是交易日的约0.3倍
        step = min(int(5000 / days * 1.3), 1000)
    else:
        step = len(ts_codes)

    res_stock_deal = DataFrame()
    batch_num = 0
    for idx in range(0, len(ts_codes), step):
        str_ts_codes = ','.join(ts_codes[idx:idx + step])
        batch = get_stock_basic(str_ts_codes, start_date, end_date)
        res_stock_deal = pd.concat([res_stock_deal, batch], ignore_index=True)

        # 批量获取历史数据时，需要分批同步
        if start_date != Date.ystday:
            batch_num += 1
            if batch_num % 2 == 0:
                Sink.df_to_mysql(res_stock_deal, table_name)
                res_stock_deal = DataFrame()
                logger.info("分批同步个股基本面数据中，总量%d个，已同步%d个 (%.2f%%)"
                            % (len(ts_codes), idx + step, (idx + step) * 100 / len(ts_codes)))

            time.sleep(13)

    return res_stock_deal


def run():
    stock = get_history_basic()
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
