with source as (
    select * from read_parquet('../data/gold/trade_stats/**/*.parquet', union_by_name=true)
),

renamed as (
    select
        symbol,
        window_start,
        window_end,
        trade_count,
        avg_trade_size,
        max_trade_size,
        trades_per_second,
        window_duration,
        partition_date,
        date_trunc('day', window_start) as trade_date
    from source
)

select * from renamed
