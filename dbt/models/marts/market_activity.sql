with daily as (
    select * from {{ ref('daily_summary') }}
),

activity as (
    select
        trade_date,
        count(distinct symbol) as active_symbols,
        sum(volume) as total_volume,
        sum(trade_count) as total_trades,
        avg(buy_ratio) * 100 as avg_buy_ratio_pct,
        sum(case when close_price > open_price then volume else 0 end) as bullish_volume,
        sum(case when close_price < open_price then volume else 0 end) as bearish_volume
    from daily
    group by trade_date
)

select * from activity
