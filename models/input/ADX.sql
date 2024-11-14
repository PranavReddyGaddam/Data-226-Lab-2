WITH base_data AS (
    SELECT
        date,
        symbol,
        high,
        low,
        close,
        LAG(high) OVER (PARTITION BY symbol ORDER BY date) AS prev_high,
        LAG(low) OVER (PARTITION BY symbol ORDER BY date) AS prev_low,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
    FROM {{ source('raw_data', 'stock_prices') }}
),

directional_movements AS (
    SELECT
        date,
        symbol,
        close,
        CASE
            WHEN high - prev_high > prev_low - low AND high - prev_high > 0 THEN high - prev_high
            ELSE 0
        END AS plus_dm,
        CASE
            WHEN prev_low - low > high - prev_high AND prev_low - low > 0 THEN prev_low - low
            ELSE 0
        END AS minus_dm,
        GREATEST(ABS(high - low), ABS(high - prev_close), ABS(low - prev_close)) AS tr
    FROM base_data
),

average_dm_tr AS (
    SELECT
        date,
        symbol,
        close,
        CASE
            WHEN COUNT(*) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) = 14 THEN AVG(plus_dm) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS avg_plus_dm,
        CASE
            WHEN COUNT(*) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) = 14 THEN AVG(minus_dm) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS avg_minus_dm,
        CASE
            WHEN COUNT(*) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) = 14 THEN AVG(tr) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS avg_tr
    FROM directional_movements
),

adx_calc AS (
    SELECT
        date,
        symbol,
        close,
        100 * (avg_plus_dm / avg_tr) AS plus_di,
        100 * (avg_minus_dm / avg_tr) AS minus_di,
        CASE
            WHEN (avg_plus_dm IS NULL OR avg_minus_dm IS NULL OR avg_tr IS NULL) THEN NULL
            WHEN (plus_di + minus_di) = 0 THEN NULL
            ELSE ROUND(100 * ABS((plus_di - minus_di) / (plus_di + minus_di)), 2)
        END AS adx
    FROM average_dm_tr
)

SELECT date, symbol, close, adx
FROM adx_calc
{% if is_incremental() %}
WHERE date > (
    SELECT MAX(date) FROM {{ this }}
    WHERE date IS NOT NULL
)
{% endif %}
ORDER BY date DESC, symbol ASC
