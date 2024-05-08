'''
Author: Van Sun
Date: 2024-04-28 15:26:00
LastEditTime: 2024-05-08 00:52:04
LastEditors: Van Sun
Description: Q4 paper:Hou, Xue, and Zhang (2015) :Digesting anomalies: An investment approach
            四因子模型（市场、市值、投资(最近一个财年 total assets 的变化率)以及盈利(ROE,也有人认为毛利率更好)）
            Q5: Hou, K., H. Mo, C. Xue, and L. Zhang (2018). q^5. NBER Working Paper, No. 24709.
            An Augmented q-factor Model with Expected Growth, Review of Finance.
FilePath: \IFactor\q_factor_model.py

'''
import arcticdb as adb
import numpy as np
from sklearn.preprocessing import StandardScaler
from arcticdb import QueryBuilder
from getTushareData import *
import datetime as dt
from mytools import *
from saveDataToArcticDB import writeDB
import alphalens as al
# 根据百分位去极值
def winsor(df,factorName: str):
    ceiling = df[factorName].quantile(0.99)
    floor = df[factorName].quantile(0.01)
    df.loc[df[factorName] >ceiling,factorName] =ceiling
    df.loc[df[factorName] < floor,factorName] = floor
    return df[factorName]
#取单因子
def get_factor_data(arc_connection,factor_name: str, begin,end):
    start_date = dt.datetime.strptime(begin,'%Y%m%d')
    end_date = dt.datetime.strptime(end,'%Y%m%d')
    q = QueryBuilder()
    q = q[ (q['trade_date']>=start_date)& (q['trade_date']<=end_date)]#f_turnover_rate_f
    from_storage_df = arc_connection.read('factor_basic', \
        columns = ['trade_date', 'ts_code',]+[factor_name], query_builder=q).data
    del q
    return from_storage_df
#将多因子合成一个单因子,未来要改造成深度学习算法
def compute_multifactor_data(arc_connection,factor_name1: str, factor_name2: str,begin,end):
    scaler = StandardScaler()
    start_date = dt.datetime.strptime(begin,'%Y%m%d')
    end_date = dt.datetime.strptime(end,'%Y%m%d')
    q = QueryBuilder()
    q = q[ (q['trade_date']>=start_date)& (q['trade_date']<=end_date)]
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

def get_stock_price_data(arc_connection, begin,end):
    start_date = dt.datetime.strptime(begin,'%Y%m%d')
    end_date = dt.datetime.strptime(end,'%Y%m%d')
    q = QueryBuilder()
    q = q[ (q['trade_date']>=start_date)& (q['trade_date']<=end_date)]#close
    from_storage_df = arc_connection.read('stock_price',query_builder=q).data
    del q
    return from_storage_df
    
if __name__ == "__main__":
    ac = adb.Arctic('lmdb://./data/IFactorDB/database?map_size=50GB')
    begin = '20100101'#Backtest from 2010-01-01
    end = '20240424'#Backtest end 2024-04-24
    library = ac['tsData'] 
    pro = ts.pro_api()  
    #单因子测试
    # from_storage_df = library.read('factor_basic').data
    # factor_df = get_factor_data(library,'s_total_mv',begin,end)
    factor_df = compute_multifactor_data(library,'total_mv,pb','q_factor_investment',begin,end)
    factor_df.set_index(['trade_date','ts_code'], inplace=True)
    #多因子测试
    
    #取股价
    stock_price_df = get_stock_price_data(library, begin,end)
    stock_price_to_alphalens = stock_price_df.pivot(index='trade_date',columns='ts_code', values='close')
    # print(stock_price_to_alphalens)
    import seaborn as sns
    factor_result = al.utils.get_clean_factor_and_forward_returns(
        factor=factor_df,
        prices=stock_price_to_alphalens,
        quantiles=10,
        periods=(1, 10, 20))
    #一种简化版的报告，省去了图表，只有统计信息
    # al.tears.create_summary_tear_sheet(factor_result, long_short=True, group_neutral=False)
    # factor_returns = al.performance.factor_returns(factor_result)
    # mean_return_by_q_daily, std_err = al.performance.mean_return_by_quantile(\
    # factor_result, by_date=False)

    # # mean_return_by_q_daily.head()
    
    
    # from alphalens.plotting import plot_quantile_returns_bar

    # plot_quantile_returns_bar(mean_return_by_q_daily)
    # sns.despine()
    
    # from alphalens.plotting import plot_quantile_returns_violin
    # #by_date=True!
    # plot_quantile_returns_violin(mean_return_by_q_daily)
    # sns.despine()
    
    # from alphalens.performance import compute_mean_returns_spread
    # from alphalens.plotting import plot_mean_quantile_returns_spread_time_series

    # qrs, ses = compute_mean_returns_spread(mean_return_by_q_daily,upper_quant=10, lower_quant=1,std_err=std_err)

    # plot_mean_quantile_returns_spread_time_series(qrs, ses)
    
    # from alphalens.plotting import plot_cumulative_returns_by_quantile

    # mean_return_by_q_daily, std_err = al.performance.mean_return_by_quantile(factor_result, by_date=True)

    # plot_cumulative_returns_by_quantile(mean_return_by_q_daily, period='20D')
    # sns.despine()
    al.tears.create_full_tear_sheet(factor_result, long_short=True, group_neutral=False, by_group=False)
    # al.tears.create_returns_tear_sheet(factor_result, long_short=True, group_neutral=False, by_group=False)

    # al.tears.create_information_tear_sheet(factor_data, group_neutral=False, by_group=False)

    # al.tears.create_turnover_tear_sheet(factor_data)

    # al.tears.create_event_returns_tear_sheet(factor_data, stock_price_df,
    #                                      avgretplot=(5, 15),
    #                                      long_short=True,
    #                                      group_neutral=False,
    #                                      std_bar=True,
    #                                      by_group=False)
    # al.tears.create_event_returns_tear_sheet(factor_data, stock_price_to_alphalens,
    #                                      avgretplot=(2,10),
    #                                      long_short=True,
    #                                      group_neutral=False,
    #                                      std_bar=False,
    #                                      by_group=False)
