with source as (
    select * from read_parquet('../data/gold/volume_profiles/**/*.parquet', union_by_name=true)
),

renamed as (
    select
        symbol,
        window_start,
        window_end,
        total_volume,
        buy_volume,
        sell_volume,
        buy_count,
        sell_count,
        vwap,
        window_duration,
        partition_date,
        date_trunc('day', window_start) as trade_date
    from source
)

select * from renamed
