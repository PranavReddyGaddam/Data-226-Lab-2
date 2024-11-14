{% snapshot adx_snapshot %}
    {{
        config(
            target_schema='analytics',
            unique_key='date || symbol',  
            strategy='timestamp',
            updated_at='date'
        )
    }}

    SELECT
        date AS date,
        symbol AS symbol,
        close AS close,
        adx AS adx
    FROM {{ ref('ADX') }}
{% endsnapshot %}

{% snapshot atr_snapshot %}
{{
        config(
            target_schema='analytics',
            unique_key='date || symbol',  
            strategy='timestamp',
            updated_at='date'
        )
    }}

    SELECT
        date AS date,
        symbol AS symbol,
        close AS close,
        atr AS atr
    FROM {{ ref('ATR') }}
{% endsnapshot %}

{% snapshot dma_snapshot %}
   {{
        config(
            target_schema='analytics',
            unique_key='date || symbol',  
            strategy='timestamp',
            updated_at='date'
        )
    }}

    SELECT
        date AS date,
        symbol AS symbol,
        close AS close,
        ma_7_day AS ma_7_day
    FROM {{ ref('DMA') }}
{% endsnapshot %}

{% snapshot rsi_snapshot %}
    {{
        config(
            target_schema='analytics',
            unique_key='date || symbol',  
            strategy='timestamp',
            updated_at='date'
        )
    }}

    SELECT
        date AS date,
        symbol AS symbol,
        close AS close,
        rsi AS rsi
    FROM {{ ref('RSI') }}
{% endsnapshot %}

{% snapshot soc_snapshot %}
    {{
        config(
            target_schema='analytics',
            unique_key='date || symbol',  
            strategy='timestamp',
            updated_at='date'
        )
    }}

    SELECT
        date AS date,
        symbol AS symbol,
        close AS close,
        highest_high AS highest_high,
        lowest_low AS lowest_low
    FROM {{ ref('SOC') }}
{% endsnapshot %}
