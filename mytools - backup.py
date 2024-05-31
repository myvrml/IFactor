'''
Author: Van Sun
Date: 2024-04-25 17:35:40
LastEditTime: 2024-05-30 13:57:34
LastEditors: Van Sun
Description: 工具类函数
FilePath: \IFactor\mytools - backup.py

'''
import dolphindb as ddb
from getTushareData import *
import datetime as dt
import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np
# def isTradingDay(dolphin_connection,day):
#     q = QueryBuilder()
#     q = q[(q["cal_date"] == day.strftime('%Y%m%d')) ]
#     from_storage_df = dolphin_connection.read('trade_cal',query_builder=q).data
#     if from_storage_df['is_open'][0]==1:
#         return True 
#     else:
#         return False
def isTradingDay(dolphin_connection,day):
    tb = dolphin_connection.table(dbPath="dfs://k_day_level", data="trade_cal")
    from_storage_df = tb.select("cal_date, is_open").where("cal_date="+day.strftime('%Y.%m.%d')).toDF()
    if from_storage_df['is_open'][0]==1:
        return True 
    else:
        return False    
# 取某一交易日所有股票的股票池及个股基本信息turnover_rate,pe,pb,total_mv,筛选股票的标准:
# 总市值小于size_threshold的全部剔除,默认为0(不剔除), 
def getStockPoolAndBasic(TushareConnection: any, tradingDay: any, size_threshold=0):
    if isinstance(tradingDay, str)==False:
        tradingDay=tradingDay.strftime('%Y%m%d')#
    #rev_yoy收入同比（%）,profit_yoy利润同比（%）,gpr毛利率（%）,npr净利润率（%）
    from_storage_df = TushareConnection.bak_basic(trade_date=tradingDay, \
        fields='trade_date,ts_code,name,industry,pe,pb,list_date,gpr,npr')
    if from_storage_df.empty:
        print(tradingDay+" 无行情数据,这一天的factor未导入.")
        return None
    else:
        from_storage_df.replace(0, np.nan, inplace=True)
        from_storage_df.dropna(axis=0, how='any', inplace=True)#剔除空值和0值
        #剔除'000043.SZ',这个票Tushare数据有错误
        from_storage_df=from_storage_df[~(from_storage_df['ts_code']=='000043.SZ')]
        #剔除st和新股
        from_storage_df=from_storage_df[~(from_storage_df['name'].apply(lambda x:x.startswith('*ST')))]
        from_storage_df=from_storage_df[~(from_storage_df['name'].apply(lambda x:x.startswith('ST')))]
        from_storage_df=from_storage_df[~(from_storage_df['name'].apply(lambda x:x.startswith('N')))]
        #剔除上市一年以内的新股次新股
        from_storage_df=from_storage_df[from_storage_df['list_date'].\
            apply(lambda x:(dt.datetime.strptime(tradingDay,'%Y%m%d')-dt.datetime.strptime(x,'%Y%m%d')).days>365)]
        
        #strength 强弱度(%) interval_3近3月涨幅 industry	所属行业 vol_ratio量比
        bak_daily_df = TushareConnection.bak_daily(trade_date=tradingDay, \
            fields='ts_code,open,total_mv,turn_over,pct_change,vol_ratio,strength')
        bak_daily_df.replace(0, np.nan, inplace=True)
        bak_daily_df.dropna(axis=0, how='any', inplace=True)#剔除空值和0值
        # 剔除停牌,即open=0,
        bak_daily_df = bak_daily_df[~(bak_daily_df['open']==0)]
        #剔除市值小于size_threshold的股票
        if size_threshold > 0:
            bak_daily_df = bak_daily_df[(bak_daily_df['total_mv'] > size_threshold)]
        #剔除涨跌停板
        bak_daily_df = bak_daily_df[(bak_daily_df['pct_change']<9.8) & (bak_daily_df['pct_change']>-9.8)]
        stock_pool_factor_basic_df = pd.merge(from_storage_df, \
            bak_daily_df[['ts_code','total_mv','turn_over','vol_ratio','strength']], on=['ts_code'], how='inner')
        return stock_pool_factor_basic_df


def getStockPool(dolphin_connection, tradingDay):
    if isinstance(tradingDay, str):
        tradingDay = dt.datetime.strptime(tradingDay,'%Y%m%d')
    tb = dolphin_connection.table(dbPath="dfs://k_day_level", data="company_detail")
    from_storage_df = tb.select("*").where("trade_date="+tradingDay.strftime('%Y.%m.%d')).toDF()
    return from_storage_df[['ts_code']]

#从Tushare中取所有股票代码,包括退市
def getAllStocks(tushare_connection):
    df1 = tushare_connection.stock_basic(exchange='', list_status='L', fields='ts_code')
    df2 = tushare_connection.stock_basic(exchange='', list_status='D', fields='ts_code')
    # df3 = tushare_connection.stock_basic(exchange='', list_status='P', fields='ts_code')
    from_storage_df = pd.concat([df1, df2], axis=0)
    df_unique = from_storage_df.drop_duplicates()
    return df_unique[['ts_code']]

def get_stock_price_data(dolphin_connection, begin,end):
    if isinstance(begin, str):
        begin = dt.datetime.strptime(begin,'%Y%m%d')
    if isinstance(end, str):
        end = dt.datetime.strptime(end,'%Y%m%d')
    tb = dolphin_connection.table(dbPath="dfs://k_day_level", data="k_day")
    from_storage_df = tb.select("trade_date,ts_code").where(f"trade_date>={begin.strftime('%Y.%m.%d')} and "\
        "trade_date<={end.strftime('%Y.%m.%d')}").toDF()
    return from_storage_df
# def get_stock_price_data(dolphin_connection, begin,end):
#     if isinstance(begin, str):
#         begin = dt.datetime.strptime(begin,'%Y%m%d')
#     if isinstance(end, str):
#         end = dt.datetime.strptime(end,'%Y%m%d')
#     q = QueryBuilder()
#     q = q[ (q['trade_date']>=begin)& (q['trade_date']<=(end + dt.timedelta(days=1)))]#close
#     from_storage_df = dolphin_connection.read('stock_price',query_builder=q).data
#     del q
#     return from_storage_df
# 根据百分位去极值
def winsor(df,factorName: str):
    ceiling = df[factorName].quantile(0.99)
    floor = df[factorName].quantile(0.01)
    df.loc[df[factorName] >ceiling,factorName] =ceiling
    df.loc[df[factorName] < floor,factorName] = floor
    return df[factorName]
#将多因子合成一个单因子,未来要改造成深度学习算法
# def compute_multifactor_data(dolphin_connection,factor_name1: str, factor_name2: str,begin,end):
#     scaler = StandardScaler()
#     if isinstance(begin,str):
#         start_date = dt.datetime.strptime(begin,'%Y%m%d')
#     else:
#         start_date = begin
#     if isinstance(end,str):
#         end_date = dt.datetime.strptime(end,'%Y%m%d')
#     else:
#         end_date = end
#     q = QueryBuilder()
#     q = q[ (q['trade_date']>=start_date)& (q['trade_date']<=(end_date + dt.timedelta(days=1)))]
#     list_factor_name = factor_name1.split(',')
#     from_factor_basic_df = dolphin_connection.read('factor_basic', \
#         columns = (['trade_date', 'ts_code',]+list_factor_name), query_builder=q).data
#     from_q_factor_investment_df = dolphin_connection.read('q_factor_investment', \
#         columns = (['trade_date', 'ts_code',]+[factor_name2]), query_builder=q).data
#     del q
#     #把factor_basic和q_factor_investment中的ST股全部剔除
#     # for code in from_factor_basic_df['ts_code']:
        
#     sorted_from_factor_basic_df = from_factor_basic_df.sort_values(by=['trade_date','ts_code'],ascending = True)
#     sorted_from_q_factor_investment_df = from_q_factor_investment_df.sort_values(by=['trade_date','ts_code'],ascending = True)
    
#     # from_factor_basic_df[factor_name2] = sorted_from_q_factor_investment_df[factor_name2]
#     list_factor_name.append(factor_name2)
#     group_df = sorted_from_factor_basic_df.groupby('trade_date')
#     list_compounded_factor = []
#     #按天来计算因子
#     for currentdate,group in group_df:
#         group[factor_name2] = sorted_from_q_factor_investment_df[factor_name2]\
#             [sorted_from_q_factor_investment_df['trade_date']==currentdate]
#         group[factor_name2] = group[factor_name2].apply(lambda x:-x)#根据因子特性来决定:对factor_name2取负数
#         #去极值
#         for name in list_factor_name:
#             winsor(group,name)
#         #标准化
#         standardized_factor = scaler.fit_transform(group[list_factor_name])
#         #这里直接每个因子等权来合成, 未来要改造成DL
#         mean_factor = np.mean(standardized_factor, axis =1)
#         # standardized_factor = scaler.fit_transform(np.array(stockBasicList['total_mv']).reshape(-1, 1))
#         list_compounded_factor = np.concatenate((list_compounded_factor, mean_factor))
        
#     result_df = sorted_from_factor_basic_df[['trade_date','ts_code']]
#     result_df['compounded_q_factor'] = list_compounded_factor
#     return result_df