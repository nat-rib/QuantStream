with source as (
    select * from read_parquet('../data/gold/ohlc/**/*.parquet', union_by_name=true)
),

renamed as (
    select
        symbol,
        window_start,
        window_end,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        trade_count,
        window_duration,
        partition_date,
        date_trunc('day', window_start) as trade_date
    from source
)

select * from renamed
