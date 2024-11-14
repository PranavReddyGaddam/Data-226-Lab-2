WITH base_data AS (
    SELECT
        date,
        symbol,
        close,
        high,
        low,
        MAX(high) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS highest_high,
        MIN(low) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS lowest_low
    FROM {{ source('raw_data', 'stock_prices') }}
),

stochastic_calc AS (
    SELECT
        date,
        symbol,
        close,
        highest_high,
        lowest_low,
        CASE
            WHEN highest_high - lowest_low = 0 THEN NULL
            ELSE ROUND(100 * ((close - lowest_low) / (highest_high - lowest_low)), 2)
        END AS stochastic_oscillator
    FROM base_data
)

SELECT *
FROM stochastic_calc
{% if is_incremental() %}
WHERE date > (
    SELECT MAX(date) FROM {{ this }}
    WHERE date IS NOT NULL
)
{% endif %}
ORDER BY date DESC, symbol ASC


