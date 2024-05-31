'''
Author: Van Sun
Date: 2024-04-23 18:27:50
LastEditTime: 2024-05-30 23:38:14
LastEditors: Van Sun
Description: 
FilePath: \IFactor\start.py

'''
'''
Author: Van Sun
Date: 2024-04-23 18:27:50
LastEditTime: 2024-05-25 22:56:31
LastEditors: Van Sun
Description: 
FilePath: \IFactor\start.py

'''
import arcticdb as adb
import numpy as np
from sklearn.preprocessing import StandardScaler
from arcticdb import QueryBuilder
from arcticdb.options import LibraryOptions
from getTushareData import *
import datetime as dt
from mytools import *
from factors import *
from saveDataToDolphinDB import writeDB,writeDaily_basic,writeTrade_cal,writeSuspend_d

# # 创建并打开数据库
# dataBase = lmdb.open("./data/IFactorDB", map_size=2147483648) #2GB 文件型数据库
# # 获取数据库句柄
# db = dataBase.open_db()
#取某一交易日市值大于size_threshold,未停牌,未涨跌停的股票基本信息,生成factor,并保存到本地DB
# def factor_basic(tushare_connection, dolphin_connection, begin, end):
#     scaler = StandardScaler()
#     # 设定开始和结束日期
#     if isinstance(begin, str)==True:
#         start_date = dt.datetime.strptime(begin,'%Y%m%d')
#     if isinstance(end, str)==True:
#         end_date = dt.datetime.strptime(end,'%Y%m%d')
#     # 循环遍历每一天
#     for single_date in range((end_date - start_date).days + 1):
#         #先判断这一天是否是交易日
#         current_date = start_date + dt.timedelta(days=single_date)
#         if isTradingDay(library,current_date): 
#             stockBasicList = getStockPoolAndBasic(tushare_connection,current_date.strftime('%Y%m%d'),size_threshold=200000)#取出未停牌且市值大于30亿元的股票
#             stockBasicList.dropna(axis=0, how='any', inplace=True)#剔除空值,空值多半是亏损的股票
#             stockBasicList['pe'] = stockBasicList['pe'].apply(lambda x:1/x)#对PE求倒数
#             stockBasicList['pe_ttm'] = stockBasicList['pe_ttm'].apply(lambda x:1/x)#对PE求倒数
#             stockBasicList['pb'] = stockBasicList['pb'].apply(lambda x:1/x)#对PE求倒数
#             stockBasicList['turnover_rate_f'] = stockBasicList['turnover_rate_f'].apply(lambda x:1/x)#对PE求倒数
#             standardized_factor = scaler.fit_transform(stockBasicList[['turnover_rate_f','pe','pb','dv_ratio','total_mv']])
#             # standardized_factor = scaler.fit_transform(np.array(stockBasicList['total_mv']).reshape(-1, 1))
#             resultList = stockBasicList[['trade_date','ts_code']]
#             resultList[['f_turnover_rate_f','f_pe','f_pb','f_dv_ratio','f_total_mv']] \
#                 = standardized_factor
#             # resultList.setindex('trade_date')
#             #存入到本地ArcticDB
#             if dolphin_connection.has_symbol('factor_basic'):
#                 dolphin_connection.append('factor_basic', resultList)
#                 print('追加数据表factor_basic'+current_date.strftime('%Y%m%d')+'已存入数据库.')
#             else:
#                 writeDB(dolphin_connection,'factor_basic', resultList)
#                 print('开始创建factor_basic'+current_date.strftime('%Y%m%d')+'已存入数据库.')
#             # print(resultList)    
#     return True
#此函数取股价时间太长,废弃,换一种方法
# def stock_price(tushare_connection, dolphin_connection, begin, end):
#     # 设定开始和结束日期
#     if isinstance(begin, str)==True:
#         start_date = dt.datetime.strptime(begin,'%Y%m%d')
#     if isinstance(end, str)==True:
#         end_date = dt.datetime.strptime(end,'%Y%m%d')
    
#     # 循环遍历每一天
#     for single_date in range((end_date - start_date).days + 1):
#         #先判断这一天是否是交易日
#         current_date = start_date + dt.timedelta(days=single_date)
#         if isTradingDay(library,current_date): 
#             codeList = getStockPool(dolphin_connection, current_date)
#             for code in codeList['ts_code']:
#                 if code == '600005.SH':
#                     print('相同代码'+code)
#                 if dolphin_connection.has_symbol('stock_price'):
#                     q = QueryBuilder()
#                     q = q[(q['ts_code'] == code) ]
#                     from_storage_df = dolphin_connection.read('stock_price',query_builder=q).data
#                     from_storage_df[from_storage_df['ts_code']==code]
#                     #先判断stock_price中是否有code这只股票,已经有了就跳过
#                     if len(from_storage_df) == 0:
#                         df = ts.pro_bar(ts_code=code, adj='qfq', start_date=begin, end_date=end)
#                         result_df = df[['trade_date','ts_code', 'close']].sort_values(by=['trade_date'],ascending=True)
                        
#                         #开始对时间轴，把close往后移一个交易日
#                         aList = np.array(result_df['close'][1:])
#                         aList = np.append(aList,aList[-1])
#                         result_df['close'] = aList
#                         dolphin_connection.append('stock_price', result_df)
#                         print('追加数据表stock_price:'+code+'已存入数据库.')
#                     else:
#                         print('相同代码'+code)
#                 else:
#                     df = ts.pro_bar(ts_code=code, adj='qfq', start_date=begin, end_date=end)
#                     # df = pro.stk_factor(ts_code=code, start_date=begin, end_date=end, \
#                     #     fields='ts_code,trade_date,pct_change')
#                     result_df = df[['trade_date','ts_code', 'close']].sort_values(by=['trade_date'],ascending=True)
#                     # result_df['close'] = result_df['pct_change'].expanding().apply(lambda x:np.prod(x/100+1)).values
#                     #开始对时间轴，把close往后移一个交易日
#                     aList = np.array(result_df['close'][1:])
#                     aList = np.append(aList,aList[-1])
#                     result_df['close'] = aList
#                     writeDB(dolphin_connection,'stock_price', result_df)
#                     print('创建数据表stock_price:'+code+'已存入数据库.')
#     return True

#取出所有正在上市,已退市和暂停上市的股票代码,再取每只股票历史前复权股价
def stock_price(tushare_connection, dolphin_connection, begin, end):
    codeList = getAllStocks(tushare_connection)
    for code in codeList['ts_code']:
        if dolphin_connection.has_symbol('stock_price'):
            q = QueryBuilder()
            q = q[(q['ts_code'] == code) ]
            from_storage_df = dolphin_connection.read('stock_price',query_builder=q).data
            from_storage_df[from_storage_df['ts_code']==code]
            df = ts.pro_bar(ts_code=code, adj='qfq', start_date=begin, end_date=end)
            if df is None:
                print(code+" 无行情数据.")
            else:
                if df.empty:
                    print(code+" 无行情数据.")
                else:
                    result_df = df[['trade_date','ts_code', 'open', 'high', 'low', 'close','vol']].sort_values(by=['trade_date'],ascending=True)
                    
                    #如果是alphalengs回测需要对时间轴，把close往后移一个交易日,如果用BT回测就不需要后移一天
                    # aList = np.array(result_df['close'][1:])
                    # aList = np.append(aList,aList[-1])
                    # result_df['close'] = aList
                    result_df['trade_date'] = result_df['trade_date'].apply(lambda x:dt.datetime.strptime(x,'%Y%m%d')).values
                    # result_df.set_index(['trade_date','ts_code'],inplace=True)
                    dolphin_connection.append('stock_price', result_df)
                    print('追加数据表stock_price:'+code+'已存入数据库.')
        else:
            df = ts.pro_bar(ts_code=code, adj='qfq', start_date=begin, end_date=end)
            # df = pro.stk_factor(ts_code=code, start_date=begin, end_date=end, \
            #     fields='ts_code,trade_date,pct_change')
            if df is None:
                print(code+" 无行情数据.")
            else:
                result_df = df[['trade_date','ts_code', 'open', 'high', 'low', 'close','vol']].sort_values(by=['trade_date'],ascending=True)
                        
                #如果是alphalengs回测需要对时间轴，把close往后移一个交易日,如果用BT回测就不需要后移一天
                # aList = np.array(result_df['close'][1:])
                # aList = np.append(aList,aList[-1])
                # result_df['close'] = aList
                result_df['trade_date'] = result_df['trade_date'].apply(lambda x:dt.datetime.strptime(x,'%Y%m%d')).values
                # result_df.set_index(['trade_date','ts_code'],inplace=True)
                writeDB(dolphin_connection,'stock_price', result_df)
                print('创建数据表stock_price:'+code+'已存入数据库.')    
    return True

def update_table(tushare_connection, dolphin_connection, table_name, begin, end):
    if table_name == 'trade_cal':
        to_append_df = getTrade_cal(begin,end)
        dolphin_connection.append("trade_cal", to_append_df)
        print('trade_cal updated.')
    if table_name == 'suspend_d':
        writeSuspend_d(dolphin_connection,begin,end)
        print('suspend_d updated.')
    if table_name == 'stock_price':
        stock_price(tushare_connection,dolphin_connection,begin,end)
        print('stock_price updated.')
    if table_name == 'factor_basic':
        factor_basic(tushare_connection, dolphin_connection, begin, end)
        print('factor_basic updated.')
    if table_name == 'q_factor_investment':
        factor_finance_indicator(tushare_connection, dolphin_connection, begin, end)
        print('q_factor_investment updated.')
        
if __name__ == "__main__":
    #建立本地数据库,数据量很大,由于ArcticDB对中文支持不好,所以把数据库迁移到DolphinDB
    # ac = adb.Arctic('lmdb://./data/IFactorDB/database?map_size=2GB')
    # # ac.create_library('my_data', library_options=LibraryOptions(dynamic_schema=True))
    # library = ac.get_library('tsData', create_if_missing=True, library_options=LibraryOptions(dynamic_schema=True))
    import dolphindb as ddb
    s = ddb.session()
    s.connect("localhost", 8848, "admin", "123456")
    begin = '20171220'#2017年之前的tushare数据质量不佳20171220
    end = '20240523'#
    #数据库建表详见createDatabase.dos
    #https://tushare.pro/register?reg=658542 ,注册Tushare账号,并交费500元以上,500元对应5000积分
    ts.set_token(environ.get("TUSHARE_TOKEN"))
    pro = ts.pro_api()
    # 以下每一步都需要很长时间,建议分段手动执行
    # writeTrade_cal(library, begin)
    # writeSuspend_d(library, begin, end)
    # library.delete('factor_basic') 
    # factor_basic(pro,s,begin,end)
    # library.delete('stock_price') 
    # stock_price(pro,library,begin,end)
    # factor_finance_indicator(pro, s, begin, end)
    update_table(pro,s,'factor_basic','20171220','20240523')
    # from_storage_df = library.read('q_factor_investment').data
    # print(from_storage_df.head())
    s.close()