'''
Author: Van Sun
Date: 2024-04-25 17:35:40
LastEditTime: 2024-05-12 19:27:38
LastEditors: Van Sun
Description: 工具类函数
FilePath: \IFactor\mytools.py

'''
import arcticdb as adb
from arcticdb import QueryBuilder
from getTushareData import *
import datetime as dt
import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np
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
# df = pro.stock_basic(exchange='', list_status='L')
#     #剔除2017年以后上市的新股次新股
#     df=df[df['list_date'].apply(int).values<20170101]
#     #剔除st股
#     df=df[-df['name'].apply(lambda x:x.startswith('*ST'))]
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
    # df3 = tushare_connection.stock_basic(exchange='', list_status='P', fields='ts_code')
    from_storage_df = pd.concat([df1, df2], axis=0)
    return from_storage_df[['ts_code']]

def get_stock_price_data(arc_connection, begin,end):
    if isinstance(begin, str):
        begin = dt.datetime.strptime(begin,'%Y%m%d')
    if isinstance(end, str):
        end = dt.datetime.strptime(end,'%Y%m%d')
    q = QueryBuilder()
    q = q[ (q['trade_date']>=begin)& (q['trade_date']<=(end + dt.timedelta(days=1)))]#close
    from_storage_df = arc_connection.read('stock_price',query_builder=q).data
    del q
    return from_storage_df

# 根据百分位去极值
def winsor(df,factorName: str):
    ceiling = df[factorName].quantile(0.99)
    floor = df[factorName].quantile(0.01)
    df.loc[df[factorName] >ceiling,factorName] =ceiling
    df.loc[df[factorName] < floor,factorName] = floor
    return df[factorName]
#将多因子合成一个单因子,未来要改造成深度学习算法
def compute_multifactor_data(arc_connection,factor_name1: str, factor_name2: str,begin,end):
    scaler = StandardScaler()
    if isinstance(begin,str):
        start_date = dt.datetime.strptime(begin,'%Y%m%d')
    else:
        start_date = begin
    if isinstance(end,str):
        end_date = dt.datetime.strptime(end,'%Y%m%d')
    else:
        end_date = end
    q = QueryBuilder()
    q = q[ (q['trade_date']>=start_date)& (q['trade_date']<=(end_date + dt.timedelta(days=1)))]
    list_factor_name = factor_name1.split(',')
    from_factor_basic_df = arc_connection.read('factor_basic', \
        columns = (['trade_date', 'ts_code',]+list_factor_name), query_builder=q).data
    from_q_factor_investment_df = arc_connection.read('q_factor_investment', \
        columns = (['trade_date', 'ts_code',]+[factor_name2]), query_builder=q).data
    del q
    sorted_from_factor_basic_df = from_factor_basic_df.sort_values(by=['trade_date','ts_code'],ascending = True)
    sorted_from_q_factor_investment_df = from_q_factor_investment_df.sort_values(by=['trade_date','ts_code'],ascending = True)
    # from_factor_basic_df[factor_name2] = sorted_from_q_factor_investment_df[factor_name2]
    list_factor_name.append(factor_name2)
    group_df = sorted_from_factor_basic_df.groupby('trade_date')
    list_compounded_factor = []
    #按天来计算因子
    for currentdate,group in group_df:
        group[factor_name2] = sorted_from_q_factor_investment_df[factor_name2]\
            [sorted_from_q_factor_investment_df['trade_date']==currentdate]
        group[factor_name2] = group[factor_name2].apply(lambda x:-x)#根据因子特性来决定:对factor_name2取负数
        #去极值
        for name in list_factor_name:
            winsor(group,name)
        #标准化
        standardized_factor = scaler.fit_transform(group[list_factor_name])
        #这里直接每个因子等权来合成, 未来要改造成DL
        mean_factor = np.mean(standardized_factor, axis =1)
        # standardized_factor = scaler.fit_transform(np.array(stockBasicList['total_mv']).reshape(-1, 1))
        list_compounded_factor = np.concatenate((list_compounded_factor, mean_factor))
        
    result_df = sorted_from_factor_basic_df[['trade_date','ts_code']]
    result_df['compounded_q_factor'] = list_compounded_factor
    return result_df