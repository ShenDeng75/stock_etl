#!/usr/bin/env python
# -*- coding: utf-8 -*-
import tushare as ts
from pandas import DataFrame

from common.properties import conf
from common.tools import Sink

ts.set_token(conf.ts_token)
pro = ts.pro_api()

table_name = 't_trade_day'


# 交易日期
def get_trade_day():
    trade_day = pro.trade_cal(start_date='20220101', end_date='20221231')
    return trade_day


def run():
    trade_day: DataFrame = get_trade_day()
    Sink.df_to_mysql(trade_day, table_name, Sink.MODEL_REPLACE)


if __name__ == "__main__":
    run()
