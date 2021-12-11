# Importacion de librerias y definicion de entorno

import yfinance as yf
import pandas as pd
from datetime import date
import requests
from prefect import task, Flow

tickers = ['NVDA', 'TSLA', 'MSFT', 'AMZN', 'AMD', 'INTC']
today = date.today()
today = today.strftime("%Y-%m-%d")

# Extraccion yfinance + API Coinbase

@task
def extract(tickers, today):
    raw_dfs = {}

    # Datos desde NASDAQ

    for ticker in tickers:
        tk =yf.Ticker(ticker)
        raw_df = pd.DataFrame(tk.history(period='1d'))
        raw_df.columns = raw_df.columns.str.lower()
        raw_df = raw_df[['open', 'high', 'low', 'close']]
        raw_dfs[ticker] = raw_df


     # Datos BTC

    response = requests.get('https://api.coinbase.com/v2/prices/spot?currency=USD')
    btc_raw = response.json()
    btc_raw = float(btc_raw['data']['amount'])
    btc_index = pd.to_datetime([today])
    btc_dct = {'btc_usd': btc_raw}
    btc_raw = pd.DataFrame(btc_dct, index=btc_index)
    raw_dfs['btc_usd'] = btc_raw

    return raw_dfs



# Transformacion
@task
def transform(raw_dfs):
    pass

# Carga
@task
def load():
    pass


# Flow
with Flow('ETL Caso') as flow:
    #raw_dfs = extract()
    #tablon = transform(raw_dfs)
    #load(tablon)
    tickers = ['NVDA', 'TSLA', 'MSFT', 'AMZN', 'AMD', 'INTC']
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    extract(tickers, today)

flow.run()



