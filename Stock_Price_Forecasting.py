import yfinance as yf
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from statsmodels.tsa.arima.model import ARIMA
from datetime import timedelta, datetime
import snowflake.connector
import requests
import pandas as pd
import numpy as np
from airflow.operators.bash import BashOperator


"""Snowflake Connection"""
def return_snowflake_conn():
    hook = SnowflakeHook(conn_id='snowflake_default')
    conn = hook.get_conn()
    return conn.cursor()
    
"""Extraction of Stock Data from YFinance."""


@task
def extract_stock_data(stock_symbol):
    # Fetch historical data for the past 90 days using yfinance
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    # Fetch the stock data using yfinance
    stock = yf.Ticker(stock_symbol)
    df = stock.history(start=start_date, end=end_date, interval="1d")
    
    # Adjust the dataframe to match your expected format
    df.reset_index(inplace=True)
    df.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
    
    # Add the 'symbol' column
    df['symbol'] = stock_symbol
    
    return df



"""Loading Data into the tables"""

@task
def load_90_days_data_to_snowflake(df):
    cur = return_snowflake_conn()

    try:
        for _, row in df.iterrows():
            open_rounded = round(row['open'], 2) if pd.notnull(row['open']) else 'NULL'
            high_rounded = round(row['high'], 2) if pd.notnull(row['high']) else 'NULL'
            low_rounded = round(row['low'], 2) if pd.notnull(row['low']) else 'NULL'
            close_rounded = round(row['close'], 2) if pd.notnull(row['close']) else 'NULL'
            volume_rounded = int(round(row['volume'])) if pd.notnull(row['volume']) else 'NULL'

            merge_query = f"""
            MERGE INTO raw_data.stock_prices AS target
            USING (
                SELECT '{row['date'].strftime('%Y-%m-%d')}' AS date,
                       {open_rounded} AS open,
                       {high_rounded} AS high,
                       {low_rounded} AS low,
                       {close_rounded} AS close,
                       {volume_rounded} AS volume,
                       '{row['symbol']}' AS symbol
            ) AS source
            ON target.date = source.date AND target.symbol = source.symbol
            WHEN MATCHED THEN
                UPDATE SET
                    open = source.open,
                    high = source.high,
                    low = source.low,
                    close = source.close,
                    volume = source.volume
            WHEN NOT MATCHED THEN
                INSERT (date, open, high, low, close, volume, symbol)
                VALUES (source.date, source.open, source.high, source.low, source.close, source.volume, source.symbol);
            """
            cur.execute(merge_query)

        cur.execute("COMMIT;")  
    except Exception as e:
        cur.execute("ROLLBACK;") 
        print(f"Error occurred: {e}")
        raise e
    finally:
        cur.close()


@task
def load_forecast_to_snowflake(forecast_df):
    cur = return_snowflake_conn()

    try:
        for _, row in forecast_df.iterrows():
            merge_query = f"""
            MERGE INTO raw_data.stock_forecasts AS target
            USING (
                SELECT '{row['date']}' AS date,
                       {row['open']} AS open,
                       {row['high']} AS high,
                       {row['low']} AS low,
                       {row['close']} AS close,
                       {row['volume']} AS volume,
                       '{row['symbol']}' AS symbol
            ) AS source
            ON target.date = source.date AND target.symbol = source.symbol
            WHEN MATCHED THEN
                UPDATE SET
                    open = source.open,
                    high = source.high,
                    low = source.low,
                    close = source.close,
                    volume = source.volume
            WHEN NOT MATCHED THEN
                INSERT (date, open, high, low, close, volume, symbol)
                VALUES (source.date, source.open, source.high, source.low, source.close, source.volume, source.symbol);
            """
            cur.execute(merge_query)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error occurred: {e}")
        raise e
    finally:
        cur.close()

"""Next 7 Days prediction"""

def getNext7WorkingDays(today):
    next_7_days = []

    day_count = 1
    for _ in range(7):
        while True:
            next_day = today + timedelta(days=day_count)
            day_count += 1
            if next_day.weekday() < 5: 
                next_7_days.append(next_day.strftime('%Y-%m-%d'))
                break  

    return next_7_days

@task
def predict_next_7_days(df):
    df['date'] = pd.to_datetime(df['date'])
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)

    df = df.sort_values(by='date')

    open_prices = df['open'].values
    high_prices = df['high'].values
    low_prices = df['low'].values
    close_prices = df['close'].values
    volume_values = df['volume'].values

    model_open = ARIMA(open_prices, order=(5, 1, 0))
    model_fit_open = model_open.fit()
    forecast_open = model_fit_open.forecast(steps=7)

    model_high = ARIMA(high_prices, order=(5, 1, 0))
    model_fit_high = model_high.fit()
    forecast_high = model_fit_high.forecast(steps=7)

    model_low = ARIMA(low_prices, order=(5, 1, 0))
    model_fit_low = model_low.fit()
    forecast_low = model_fit_low.forecast(steps=7)

    model_close = ARIMA(close_prices, order=(5, 1, 0))
    model_fit_close = model_close.fit()
    forecast_close = model_fit_close.forecast(steps=7)

    model_volume = ARIMA(volume_values, order=(5, 1, 0))
    model_fit_volume = model_volume.fit()
    forecast_volume = model_fit_volume.forecast(steps=7)

    last_date = df['date'].max()
    today = datetime.today()
    future_dates = getNext7WorkingDays(today)

    forecast_df = pd.DataFrame({
        'date': future_dates,
        'open': forecast_open,
        'high': forecast_high,
        'low': forecast_low,
        'close': forecast_close,
        'volume': np.ceil(forecast_volume),
        'symbol': df['symbol'].iloc[0]
    })

    forecast_df[['open', 'high', 'low', 'close', 'volume']] = forecast_df[['open', 'high', 'low', 'close', 'volume']].round(2)

    return forecast_df


# DAG definition
with DAG(
    dag_id='Stock_Forecast',
    start_date=datetime(2024, 10, 12),
    catchup=False,
    schedule_interval='31 0 * * *',
    tags=['ETL']
) as dag:
    stock_symbols = ["CVX", "XOM"]

    for stock_symbol in stock_symbols:
        stock_data = extract_stock_data(stock_symbol)
        load_90_days_data = load_90_days_data_to_snowflake(stock_data)
        forecast_data = predict_next_7_days(stock_data)
        load_forecast = load_forecast_to_snowflake(forecast_data)

    run_all_dbt_models = BashOperator(
    task_id='run_all_dbt_models',
    bash_command='cd /opt/airflow/stock_prices && dbt run')

    run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command='cd /opt/airflow/stock_prices && dbt test')
    
    run_dbt_snapshots = BashOperator(
    task_id='run_dbt_snapshots',
    bash_command='cd /opt/airflow/stock_prices && dbt snapshot')

    load_90_days_data >> forecast_data >> load_forecast
    load_forecast >> run_all_dbt_models
    run_all_dbt_models >> run_dbt_tests >> run_dbt_snapshots

