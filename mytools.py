'''
Author: Van Sun
Date: 2024-04-25 17:35:40
LastEditTime: 2024-05-04 11:15:25
LastEditors: Van Sun
Description: 工具类函数
FilePath: \IFactor\mytools.py

'''
import arcticdb as adb
from arcticdb import QueryBuilder
from getTushareData import *
import datetime as dt
import pandas as pd
def isTradingDay(database,day):
    q = QueryBuilder()
    q = q[(q["cal_date"] == day.strftime('%Y%m%d')) ]
    from_storage_df = database.read('trade_cal',query_builder=q).data
    if from_storage_df['is_open'][0]==1:
        return True 
    else:
        return False
    
# 取某一交易日所有股票的股票池及个股基本信息turnover_rate,pe,pb,total_mv,筛选股票的标准:
# 总市值小于size_threshold的全部剔除,默认为0(不剔除), Tushare没有给出ST的股票,所以剔除总市值小于20亿基本上已经剔除大部分ST股
# 停牌问题:从daily_basic取出的股票已经是未停牌股票
# 此函数数据源有两种:1.本地ArcticDB;2.Tushare,哪个快用哪个
def getStockPoolAndBasic(database: any, tradingDay: any, size_threshold=0):
    if isinstance(tradingDay, str)==False:
        tradingDay=tradingDay.strftime('%Y%m%d')#
    from_storage_df = database.query('daily_basic', ts_code='', trade_date=tradingDay,
                   fields='ts_code,trade_date,turnover_rate,pe,pe_ttm,pb,dv_ratio,total_mv')
    if size_threshold > 0:
        from_storage_df = from_storage_df[(from_storage_df['total_mv'] > size_threshold) & \
            (from_storage_df['turnover_rate']!=0) & (from_storage_df['pe']!=0) & (from_storage_df['pe_ttm']!=0)\
                 & (from_storage_df['pb']!=0)]
    return from_storage_df
#从ArcDB中取某一交易日的所有股票代码
def getStockPool(arc_connection, tradingDay):
    q = QueryBuilder()
    if isinstance(tradingDay, str):
        q = q[(q["trade_date"] == tradingDay) ]
    else:
        q = q[(q["trade_date"] == tradingDay.strftime('%Y%m%d')) ]#
    from_storage_df = arc_connection.read('factor_basic',query_builder=q).data
    return from_storage_df[['ts_code']]

#从Tushare中取所有股票代码,包括退市
def getAllStocks(tushare_connection):
    df1 = tushare_connection.stock_basic(exchange='', list_status='L', fields='ts_code')
    df2 = tushare_connection.stock_basic(exchange='', list_status='D', fields='ts_code')
    df3 = tushare_connection.stock_basic(exchange='', list_status='P', fields='ts_code')
    from_storage_df = pd.concat([df2, df3], axis=0)
    return from_storage_df[['ts_code']]
