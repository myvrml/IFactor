'''
Author: Van Sun
Date: 2024-04-25 17:31:19
LastEditTime: 2024-05-30 23:39:01
LastEditors: Van Sun
Description: 清洗数据并生成factors: 总市值, EB, EP, ROE,
FilePath: \IFactor\factors.py

'''
import arcticdb as adb
import numpy as np
from sklearn.preprocessing import StandardScaler
from arcticdb import QueryBuilder
from getTushareData import *
import time as T
from mytools import *
from saveDataToArcticDB import writeDB
from datetime import *; from dateutil.relativedelta import *
import calendar
#在本地建立数据库文件



#取某一交易日市值大于size_threshold,未停牌,未涨跌停的股票基本信息,生成factor,并保存到本地DB
def factor_basic(tushare_connection, dolphin_connection, begin, end):
    # 设定开始和结束日期
    if isinstance(begin, str)==True:
        start_date = datetime.strptime(begin,'%Y%m%d')
    if isinstance(end, str)==True:
        end_date = datetime.strptime(end,'%Y%m%d')
    # 循环遍历每一天
    for single_date in range((end_date - start_date).days + 1):
        #先判断这一天是否是交易日
        current_date = start_date + timedelta(days=single_date)
        if isTradingDay(dolphin_connection,current_date): #
            stockBasicList = getStockPoolAndBasic(tushare_connection,current_date.strftime('%Y%m%d'),\
                size_threshold=15)#取出未停牌且市值大于20亿元的股票
            if stockBasicList is None or stockBasicList.empty==True:
                continue
            else:
                stockBasicList.replace(0, np.nan, inplace=True)
                stockBasicList.dropna(axis=0, how='any', inplace=True)#剔除空值,空值多半是亏损的股票
                #计算ROE=PB/PE
                stockBasicList['roe'] = stockBasicList.apply(lambda x: x['pb']/x['pe'], axis=1)
                stockBasicList['pe'] = stockBasicList['pe'].apply(lambda x:1/x)#对PE求倒数
                # stockBasicList['pe_ttm'] = stockBasicList['pe_ttm'].apply(lambda x:1/x)#对pe_ttm求倒数
                stockBasicList['pb'] = stockBasicList['pb'].apply(lambda x:1/x)#对PB求倒数
                stockBasicList['total_mv'] = stockBasicList['total_mv'].apply(lambda x:-x)#对total_mv取负值
                stockBasicList['turn_over'] = stockBasicList['turn_over'].apply(lambda x:1/x)#对turnover_rate求倒数
                stockBasicList['trade_date'] = pd.to_datetime(stockBasicList['trade_date'])
                stockBasicList['list_date'] = pd.to_datetime(stockBasicList['list_date'])
                #存入DolphinDB,分两个表存储
                company_detail_df = stockBasicList[['ts_code','trade_date','name','industry','list_date']]
                appender = ddb.TableAppender(dbPath="dfs://k_day_level",tableName="company_detail", ddbSession=dolphin_connection)
                num = appender.append(company_detail_df)
                print('存入到company_detail共'+str(num)+'行')
                # factor_list = []
                # for i in range(len(stockBasicList.columns.values)):
                #     if stockBasicList.columns.values[i] not in ['ts_code','trade_date','name','industry','list_date']:
                #         factor_list.append(stockBasicList.columns.values[i])
                # factor_dolphin_df = pd.DataFrame(columns=['trade_date','ts_code','value','factorname'])
                # for i in range(len(stockBasicList)):
                #     for factor_name in factor_list:
                #         factor_dolphin_df=pd.concat([factor_dolphin_df,\
                #             pd.DataFrame([[stockBasicList.at[i,'trade_date'],stockBasicList.at[i,'ts_code'],\
                #                 stockBasicList.at[i,factor_name],factor_name]])], axis=0)
                #     print('好慢啊'+str(i)+'行')    
                factor_dolphin_df = stockBasicList[['trade_date','ts_code','pe','pb','gpr','npr'\
                    ,'total_mv','turn_over','vol_ratio','strength','roe']]
                appender = ddb.TableAppender(dbPath="dfs://dayFactorDB",tableName="factor_basic", ddbSession=dolphin_connection)
                num = appender.append(factor_dolphin_df)
                print('存入到factor_basic共'+str(num)+'行')
                print('追加数据表factor_basic'+current_date.strftime('%Y%m%d')+'已存入数据库.')
                # #存入到本地ArcticDB
                # if dolphin_connection.has_symbol('factor_basic'):
                #     dolphin_connection.append('factor_basic', stockBasicList['name'])
                #     print('追加数据表factor_basic'+current_date.strftime('%Y%m%d')+'已存入数据库.')
                # else:
                #     writeDB(dolphin_connection,'factor_basic', stockBasicList['name'])
                #     print('开始创建factor_basic'+current_date.strftime('%Y%m%d')+'已存入数据库.')
              
  
#取出财务指标并合并到factor_basic表中,此函数要求factor_basic必须已经存在且有数据
# 1.先计算q_factor中的investment因子
#里面的因子数据是根据财报数据计算出来的原始值,未做标准化处理
def factor_finance_indicator(tushare_connection, dolphin_connection, begin, end):
    # 设定开始和结束日期
    if isinstance(begin, str)==True:
        start_date = datetime.strptime(begin,'%Y%m%d')
    if isinstance(end, str)==True:
        end_date = datetime.strptime(end,'%Y%m%d')
    # q = QueryBuilder()
    # q = q[ (q['trade_date']>=start_date)& (q['trade_date']<=(end_date + dt.timedelta(days=1)))]#
    # from_storage_df = dolphin_connection.read('factor_basic', \
    #      query_builder=q,columns=['trade_date','ts_code']).data
    tb = dolphin_connection.table(dbPath="dfs://dayFactorDB", data="factor_basic")
    from_storage_df = tb.select("trade_date,ts_code").where("trade_date>="+start_date.strftime('%Y.%m.%d'))\
        .where("trade_date<="+end_date.strftime('%Y.%m.%d')).toDF()
    sorted_from_storage_df = from_storage_df.sort_values(by=['ts_code','trade_date'],ascending = True)
    group_from_storage_df = from_storage_df.groupby('ts_code')
    list_investment_YoY = []
    for code,group in group_from_storage_df:
        df = tushare_connection.balancesheet(ts_code=code, start_date='20140101', end_date=end, \
                fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_assets')
        # T.sleep(0.3)#Tushare 规定每分钟查询不能超过200次,所以update的时候要控制一下速度
        df['ann_date'] = df['ann_date'].apply(lambda x:datetime.strptime(x,'%Y%m%d')).values
        df['f_ann_date'] = df['f_ann_date'].apply(lambda x:datetime.strptime(x,'%Y%m%d')).values
        df['end_date'] = df['end_date'].apply(lambda x:datetime.strptime(x,'%Y%m%d')).values
        df = df.sort_values(by='f_ann_date',ascending = True)
        for current_date in group['trade_date']:
            #先计算当期的总资产,再计算上一期的总资产,从而算出增长率
            result_df = df[df['f_ann_date']<=current_date]   
            if  len(result_df)==0:
                investment_YoY = 0
            else:
                one_year_before = current_date+relativedelta(years=-1)
                lastday_one_year_before = datetime(one_year_before.year,12,31)
                two_year_before = current_date+relativedelta(years=-2)
                lastday_two_year_before = datetime(two_year_before.year,12,31)
                three_year_before = current_date+relativedelta(years=-3)
                lastday_three_year_before = datetime(three_year_before.year,12,31)
                if current_date.year == result_df['f_ann_date'].iloc[-1].year:#如果当天已经公布了去年年报
                    asset_one_year_before = result_df['total_assets'][result_df['end_date']==lastday_one_year_before]
                    asset_two_year_before = result_df['total_assets'][result_df['end_date']==lastday_two_year_before]
                    if (len(asset_one_year_before) ==0 or len(asset_two_year_before) ==0):
                        investment_YoY = 0
                    else:
                        investment_YoY = (asset_one_year_before.iloc[-1]-asset_two_year_before.iloc[-1])/asset_two_year_before.iloc[-1]
                else:#如果当天还没有公布去年的年报,则取两年前和三年前的年报
                    asset_two_year_before = result_df['total_assets'][result_df['end_date']==lastday_two_year_before]
                    asset_three_year_before = result_df['total_assets'][result_df['end_date']==lastday_three_year_before]
                    if (len(asset_three_year_before) ==0 or len(asset_two_year_before) ==0):
                        investment_YoY = 0
                    else:
                        investment_YoY = (asset_two_year_before.iloc[-1]-asset_three_year_before.iloc[-1])/asset_three_year_before.iloc[-1]
                        
            list_investment_YoY.append(investment_YoY)
        print(code+'investment因子已计算完成')
    sorted_from_storage_df['value'] = list_investment_YoY
    #存入到本地Dolphin,改为行factor
    sorted_from_storage_df['factorname'] = 'q_factor_investment'
    appender = ddb.TableAppender(dbPath="dfs://dayFactorDB",tableName="factor_computed", ddbSession=dolphin_connection)
    num = appender.append(sorted_from_storage_df)
    print('存入到factor_computed共'+str(num)+'行')
    print('追加数据表factor_computed,从'+begin+"到"+end+',已存入factorName=q_factor_investment.')
    # #存入到本地ArcticDB
    # if dolphin_connection.has_symbol('q_factor_investment'):
    #     # dolphin_connection.append('q_factor_investment', sorted_from_storage_df[['ts_code'],['trade_date'],['q_factor_investment']])
    #     dolphin_connection.append('q_factor_investment', sorted_from_storage_df)
    #     print('追加数据表q_factor_investment'+current_date.strftime('%Y%m%d')+'已存入数据库.')
    # else:
    #     writeDB(dolphin_connection, 'q_factor_investment', sorted_from_storage_df)
    #     print('开始创建q_factor_investment'+current_date.strftime('%Y%m%d')+'已存入数据库.')
    
    
        
# if __name__ == "__main__":
#     ac = adb.Arctic('lmdb://./data/IFactorDB/database?map_size=20GB')
#     begin = '20171201'#Backtest from 20171201
#     end = '20240424'#Backtest to 2024-04-24
  
#     library = ac['tsData'] 
    
#     pro = ts.pro_api()  
#     # library.delete('factor_basic') 
#     factor_basic(pro, library, begin, end)
#     # library.delete('q_factor_investment') 
#     # factor_finance_indicator(pro, library, begin, end)
#     # stock_price(pro,library,begin,end)
#     # from_storage_df = library.read('factor_basic').data
#     # print(from_storage_df)