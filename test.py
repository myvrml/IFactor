import arcticdb as adb
import numpy as np
from arcticdb import QueryBuilder
import tushare as ts
import datetime 
import pandas as pd
from saveDataToArcticDB import writeDB,writeDaily_basic,writeTrade_cal,writeSuspend_d

ac = adb.Arctic('lmdb://./data/IFactorDB/database?map_size=20GB')

library = ac.get_library('tsData', create_if_missing=True)
#https://tushare.pro/register?reg=658542 ,注册Tushare账号,并交费200元以上,200元对应2000积分
# ts.set_token(environ.get("TUSHARE_TOKEN"))
# pro = ts.pro_api()  
pro = ts.pro_api('8d67e1887efab71abce6c4b171a7937d237898915ad17f8718a510f8')
begin = datetime.datetime(2021, 7, 1)#Backtest from 2010-01-01
# begin = datetime.datetime(2023, 12, 10)#Backtest from 2010-01-01
end = datetime.datetime(2021, 7, 1)#Backtest end 2024-04-24
# daily_price = get_stock_price_data(library, begin,end)
# daily_price = []
trade_info = pd.read_csv("sqm_position.csv",sep=',')
stocklist =''
priceList = []
codeList =[]
resultDf = pd.DataFrame()
for stock in trade_info['sec_code']:
    # stocklist=stocklist+','+stock
    # q = QueryBuilder()
    # q = q[ (q['trade_date']>=begin)& (q['trade_date']<=(end + datetime.timedelta(days=1)))\
    #     & (q['ts_code']==stock)]#close
    # from_storage_df = library.read('stock_price',query_builder=q).data
    df = pro.daily(ts_code=stock, start_date='20210701', end_date='20210701', fields='ts_code, trade_date, close')
    resultDf=pd.concat([resultDf,df],axis=0)
resultDf.to_csv('gethisPrice.csv')
print(resultDf)
    # print(from_storage_df)