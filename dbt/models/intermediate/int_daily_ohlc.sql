{{ config(materialized='ephemeral') }}

with ohlc as (
    select * from {{ ref('stg_ohlc') }}
),

daily as (
    select
        symbol,
        trade_date,
        min(window_start) as first_window,
        max(window_end) as last_window,
        first(open_price order by window_start) as open_price,
        max(high_price) as high_price,
        min(low_price) as low_price,
        last(close_price order by window_start) as close_price,
        sum(volume) as volume,
        sum(trade_count) as trade_count
    from ohlc
    group by symbol, trade_date
)

select * from daily
