from extract import raw_dfs, tickers, today

import pandas as pd
import numpy as np

raw_dfs = raw_dfs.copy()
tickers = tickers.copy()

# El mantenimiento de la calidad se va a hacer durante el flow, en transformacion
# vamos a hacer primero la ingenieria de caracteristicas, crearemos las variables
# diferencia apertura, cierre, rango del dia y el signo del dia de las acciones y btc
# el signo es un resultado de la apertura y el cierre

for ticker in tickers:
    df = raw_dfs[ticker]
    #Creamos una variable dentro del df
    df['dif_apert_cierre'] = df['open'] - df['close']
    df['rango_dia'] = df['high'] - df['low']
    #Hacemos un IF ELSE con Numpy
    df['signo_dia'] = np.where(df['dif_apert_cierre']>0.0, '+',  np.where(df['dif_apert_cierre'] < 0.0, '-', '0'))
    #Nos quedamos con solo estos datos del df
    df = df[['close', 'dif_apert_cierre', 'rango_dia', 'signo_dia']]
    df.columns = map(lambda x: ticker + '_' + x, df.columns.to_list())
    
    raw_dfs[ticker] = df


# Agregado

# La funcion concat de pandas toma una lista de df
# y los junta, va a hacer una agregacion horizontal,
# que tome las columnas de todos los data frames en una lista

#Primero creamos un listado de data frames

dfs_list = raw_dfs.values()

tablon = pd.concat(dfs_list, axis=1)

print('----------------------------')
print(tablon)