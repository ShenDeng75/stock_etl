#!/usr/bin/env python
# -*- coding: utf-8 -*-
import tushare as ts
from pandas import DataFrame

from common.tools import Sink

ts.set_token("c20ea165fe87e91c0eec2f1fb529b29e0a1ce3ee61f017dc96f64c7b")
pro = ts.pro_api()

table_name = 't_trade_day'


# 交易日期
def get_trade_day():
    trade_day = pro.trade_cal(start_date='20220101', end_date='20221231')
    return trade_day


def run():
    trade_day: DataFrame = get_trade_day()
    Sink.df_to_mysql(trade_day, table_name)


if __name__ == "__main__":
    run()
