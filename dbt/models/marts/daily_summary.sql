with daily_ohlc as (
    select * from {{ ref('int_daily_ohlc') }}
),

symbol_metrics as (
    select * from {{ ref('int_symbol_metrics') }}
)

select
    coalesce(o.symbol, s.symbol) as symbol,
    coalesce(o.trade_date, s.trade_date) as trade_date,
    o.open_price,
    o.high_price,
    o.low_price,
    o.close_price,
    o.volume,
    o.trade_count,
    s.buy_volume,
    s.sell_volume,
    s.buy_count,
    s.sell_count,
    s.avg_vwap,
    s.buy_ratio
from daily_ohlc o
full outer join symbol_metrics s
    on o.symbol = s.symbol
    and o.trade_date = s.trade_date
