#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time

import tushare as ts
import pandas as pd
from pandas import DataFrame
from retrying import retry

from common.logger import logger
from common.properties import conf
from common.tools import Sink

pd.set_option('display.width', 200)
pd.set_option('display.max_columns', None)

ts.set_token(conf.ts_token)
pro = ts.pro_api()

table_name = 't_stock_detail'


# 个股详情(最多重试5次，每次间隔3秒)
@retry(stop_max_attempt_number=5, wait_fixed=3000)
def get_stock_detail() -> DataFrame:
    stock_df: DataFrame = pro.stock_basic(
        fields='ts_code,symbol,name,area,industry,market,exchange,curr_type,'
               'list_status,list_date,delist_date,is_hs')
    rows = stock_df.shape[0]
    logger.info("从 pro.stock_basic接口获取个股详情数据 %d行" % rows)
    dt = time.strftime('%Y%m%d')
    dt_col = [dt for _ in range(rows)]
    # 原始数据中没有时间列
    stock_df.insert(loc=0, column='dt', value=dt_col)

    return stock_df


def run():
    stock = get_stock_detail()
    Sink.df_to_mysql(stock, table_name, Sink.MODEL_REPLACE)


if __name__ == "__main__":
    run()
