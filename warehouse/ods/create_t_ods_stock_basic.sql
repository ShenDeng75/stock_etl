create table if not exists stock.t_ods_stock_basic
(
    input_time   varchar(16) comment '集成时间',
    ts_code      varchar(16) comment '股票代码',
    trade_date   varchar(16) comment '交易日期',
    name         varchar(255) comment '股票名称',
    pct_change   float comment '涨跌幅',
    `close`      float comment '收盘价',
    `change`     float comment '涨跌额',
    `open`       float comment '开盘价',
    high         float comment '最高价',
    low          float comment '最低价',
    pre_close    float comment '昨收价',
    vol_ratio    float comment '量比',
    turn_over    float comment '换手率',
    swing        float comment '振幅',
    vol          float comment '成交量',
    amount       float comment '成交额',
    selling      float comment '内盘（主动卖，手）',
    buying       float comment '外盘（主动买， 手）',
    total_share  float comment '总股本(亿)',
    float_share  float comment '流通股本(亿)',
    pe           float comment '市盈(动)',
    industry     varchar(255) comment '所属行业',
    area         varchar(255) comment '所属地域',
    float_mv     float comment '流通市值',
    total_mv     float comment '总市值',
    avg_price    float comment '平均价',
    strength     float comment '强弱度(%)',
    activity     float comment '活跃度(%)',
    avg_turnover float comment '笔换手',
    attack       float comment '攻击波(%)',
    interval_3   float comment '近3月涨幅',
    interval_6   float comment '近6月涨幅'
) partitioned by (dt string)
    row format delimited fields terminated by '\001';