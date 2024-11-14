WITH base_data AS (
    SELECT
        date,
        symbol,
        high,
        low,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
        ABS(high - low) AS true_range_1,
        ABS(high - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) AS true_range_2,
        ABS(low - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) AS true_range_3
    FROM {{ source('raw_data', 'stock_prices') }}
),

true_range_calc AS (
    SELECT
        date,
        symbol,
        close,
        GREATEST(true_range_1, true_range_2, true_range_3) AS true_range
    FROM base_data
),

average_true_range AS (
    SELECT
        date,
        symbol,
        close,
        AVG(true_range) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS atr
    FROM true_range_calc
)

SELECT *
FROM average_true_range
{% if is_incremental() %}
WHERE date > (
    SELECT MAX(date) FROM {{ this }}
    WHERE date IS NOT NULL
)
{% endif %}
ORDER BY date DESC, symbol ASC
