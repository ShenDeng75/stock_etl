#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time

import tushare as ts
import pandas as pd
from pandas import DataFrame
from retrying import retry

from common.logger import logger
from common.tools import Sink

pd.set_option('display.width', 200)
pd.set_option('display.max_columns', None)

ts.set_token("c20ea165fe87e91c0eec2f1fb529b29e0a1ce3ee61f017dc96f64c7b")
pro = ts.pro_api()

table_name = 't_company_detail'


# 公司详情(最多重试5次，每次间隔3秒)
@retry(stop_max_attempt_number=5, wait_fixed=3000)
def get_company_detail() -> DataFrame:
    cmp_df: DataFrame = pro.stock_company(
        fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,'
               'province,city,introduction,website,employees,main_business,business_scope')
    rows = cmp_df.shape[0]
    logger.info("从 pro.stock_company接口获取公司详情数据 %d行" % rows)
    dt = time.strftime('%Y%m%d')
    dt_col = [dt for _ in range(rows)]
    # 原始数据中没有时间列
    cmp_df.insert(loc=0, column='dt', value=dt_col)

    return cmp_df


def run():
    company = get_company_detail()
    Sink.df_to_mysql(company, table_name, Sink.MODEL_REPLACE)


if __name__ == "__main__":
    run()
