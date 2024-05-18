'''
Author: Van Sun
Date: 2024-05-13 22:17:25
LastEditTime: 2024-05-13 23:14:01
LastEditors: Van Sun
Description: 
FilePath: \IFactor\analyz_out.py

'''
import quantstats as qs
import datetime as dt
import pandas as pd
import numpy as np
# %matplotlib inline
# extend pandas functionality with metrics, etc.
qs.extend_pandas()

# fetch the daily returns for a stock
df = pd.read_csv('dailyReturn_BT_20220209-20240424.csv',sep=',')
pd.to_datetime(df['date'])
df.set_index('date',inplace=True)
stock = df
# show sharpe ratio
print('sharpe:'+str(qs.stats.sharpe(stock)))
print('max_drawdown:'+str(qs.stats.max_drawdown(stock)))
print('win_rate '+str(qs.stats.win_rate(stock)))
print('volatility '+str(qs.stats.volatility(stock)))
print('win_loss_ratio:'+str(qs.stats.win_loss_ratio(stock)))
# print('information_ratio:'+str(qs.stats.information_ratio(stock)))



# or using extend_pandas() :)
# stock.sharpe()

# qs.plots.snapshot(stock, title='My modle', show=True)

# can also be called via:
# stock.plot_snapshot(title='Facebook Performance', show=True)

# (benchmark can be a pandas Series or ticker)
qs.reports.html(stock,benchmark='SPY')