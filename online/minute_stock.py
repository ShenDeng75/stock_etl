#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time

import pandas as pd
import akshare as ak
import schedule
from pandas import DataFrame

from common.logger import logger
from common.properties import Fields
from common.tools import Sink, Source
from offline import year_trade_day

pd.set_option('display.width', 200)
pd.set_option('display.max_columns', None)


# 获取实时股票数据
def get_real_time_data() -> str:
    df = ak.stock_zh_a_spot_em()
    row = df.shape[0]
    logger.info("从 akshare.stock_zh_a_spot_em接口获取个股数据 %d行" % row)

    dt = time.strftime('%Y-%m-%d %H:%M:%S')
    dt_col = [dt for _ in range(row)]
    # 原始数据中没有时间列
    df.insert(loc=0, column='时间', value=dt_col)
    # 删除不需要的列
    df = df.drop(columns='序号')
    res_df: DataFrame = df.rename(Fields.real_data_columns, axis=1)
    res_json_str = res_df.to_json(orient='records', force_ascii=False)
    return res_json_str


def run():
    res_json_str = get_real_time_data()
    # Sink.json_str_to_kafka(res_json_str)
    logger.info(res_json_str)


# 调度
def execute():
    ds = time.strftime('%Y%m%d')
    df_trade_day = Source.mysql_args2df(year_trade_day.table_name, where={'cal_date': ds}, columns=['is_open'])
    if df_trade_day.shape[0] != 1:
        logger.error('交易日期获取失败')
        return -1
    is_trade_day = df_trade_day.iloc[0, 0]
    if not is_trade_day:
        logger.info('%s 不是交易日' % ds)
        return 0

    # 定时调度
    logger.info("-----执行调度%s-----" % ds)
    end_time = '15:02'
    schedule.every(1).minute.until(end_time).do(run)
    is_run = True
    while is_run:
        schedule.run_pending()
        dt = time.strftime('%H:%M')
        if dt >= end_time:
            is_run = False
    logger.info("-----结束调度%s-----" % ds)


if __name__ == "__main__":
    execute()
