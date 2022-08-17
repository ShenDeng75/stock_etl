insert overwrite table stock.t_ods_stock_deal partition (dt = "${dt}")
select adj,
       input_time,
       ts_code,
       trade_date,
       `open`,
       high,
       low,
       `close`,
       pre_close,
       `change`,
       pct_chg,
       vol,
       amount
from stock.t_ext_stock_deal
where dt = "${dt}"