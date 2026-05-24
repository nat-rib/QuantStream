with daily as (
    select * from {{ ref('daily_summary') }}
),

ranked as (
    select
        symbol,
        trade_date,
        volume,
        trade_count,
        close_price,
        high_price - low_price as price_range,
        (high_price - low_price) / nullif(close_price, 0) * 100 as volatility_pct,
        buy_ratio * 100 as buy_ratio_pct,
        row_number() over (partition by trade_date order by volume desc) as volume_rank,
        row_number() over (partition by trade_date order by trade_count desc) as activity_rank
    from daily
)

select * from ranked
