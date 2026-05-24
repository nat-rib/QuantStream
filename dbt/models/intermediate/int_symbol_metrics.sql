{{ config(materialized='ephemeral') }}

with volume as (
    select * from {{ ref('stg_volume_profiles') }}
),

symbol_daily as (
    select
        symbol,
        trade_date,
        sum(total_volume) as total_volume,
        sum(buy_volume) as buy_volume,
        sum(sell_volume) as sell_volume,
        sum(buy_count) as buy_count,
        sum(sell_count) as sell_count,
        avg(vwap) as avg_vwap,
        sum(buy_volume) / nullif(sum(total_volume), 0) as buy_ratio
    from volume
    group by symbol, trade_date
)

select * from symbol_daily
