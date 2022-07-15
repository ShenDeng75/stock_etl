# 公共的维度数据


from common import config

# 是否开发模式，用来切换配置参数
is_dev = True


# 字段数据
class Fields:
    real_data_columns = {"时间": "dt", "序号": "no", "代码": "code", "名称": "name", "最新价": "price", "涨跌幅": "change_rate",
                         "涨跌额": "chang_money", "成交量": "make_cnt", "成交额": "make_money", "振幅": "swing", "最高": "max_price",
                         "最低": "min_price", "今开": "start_price", "昨收": "pre_end", "量比": "quantity_rate",
                         "换手率": "change_head", "市盈率-动态": "pe_d", "市净率": "pb", "总市值": "total_money",
                         "流通市值": "market_money", "涨速": "up_speed", "5分钟涨跌": "change_5min", "60日涨跌幅": "change_60day",
                         "年初至今涨跌幅": "chang_year"}


# 配置信息
class Config:
    @staticmethod
    def sink_config():
        if is_dev:
            return config.ConfigDev
        else:
            return config.ConfigPro
