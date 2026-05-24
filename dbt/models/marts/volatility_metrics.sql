with daily as (
    select * from {{ ref('daily_summary') }}
),

volatility as (
    select
        symbol,
        trade_date,
        close_price,
        volume,
        avg(close_price) over (partition by symbol order by trade_date rows between 6 preceding and current row) as sma_7d,
        stddev(close_price) over (partition by symbol order by trade_date rows between 6 preceding and current row) as stddev_7d,
        stddev(close_price) over (partition by symbol order by trade_date rows between 6 preceding and current row)
            / nullif(avg(close_price) over (partition by symbol order by trade_date rows between 6 preceding and current row), 0) * 100 as volatility_7d_pct
    from daily
)

select * from volatility
