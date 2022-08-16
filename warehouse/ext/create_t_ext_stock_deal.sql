create external table if not exists stock.t_ext_stock_deal
(
    adj        varchar(16) comment '复权类型 none未复权 qfq前复权 hfq后复权',
    input_time varchar(16) comment '集成时间',
    ts_code    varchar(16) comment '股票代码',
    trade_date varchar(16) comment '交易日期',
    `open`     float comment '开盘价',
    high       float comment '最高价',
    low        float comment '最低价',
    `close`    float comment '收盘价',
    pre_close  float comment '昨收价(前复权)',
    `change`   float comment '涨跌额',
    pct_chg    float comment '涨跌幅 （未复权，如果是复权请用 通用行情接口 ）',
    vol        float comment '成交量 （手）',
    amount     float comment '成交额 （千元）'
) partitioned by (dt string)
    row format delimited fields terminated by '\001';
-- location '/hdfs/path'  外部分区表可以不用设置location地址，而是在创建分区时指定
