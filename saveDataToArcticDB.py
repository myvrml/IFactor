'''
Author: Van Sun
Date: 2024-04-23 11:38:02
LastEditTime: 2024-05-19 18:24:25
LastEditors: Van Sun
Description: 将Tushare数据(账号积分要2000以上,注册地址:https://tushare.pro/register?reg=658542)导入到本地ArcticDB,
             数据量很大,需要时间1-2天,强烈建议手动单步执行此文件.
FilePath: \IFactor\saveDataToArcticDB.py

'''
import arcticdb as adb
from arcticdb import QueryBuilder
from getTushareData import *
from mytools import *
import datetime as dt

def initDB():
    ac = adb.Arctic('lmdb://./data/IFactorDB/database??map_size=20GB')
    ac.create_library('tsData')
    
def writeDB(dataBase, tableName: str, dataFrame):
    dataBase.write(tableName, dataFrame)
    
# 个股基本信息表
def writeStock_basic(dataBase):
    writeDB(dataBase, 'stock_basic', getStock_basic())
    print('stock_basic已存入数据库.')
# 交易日表    
def writeTrade_cal(dataBase, begin, end):
    writeDB(dataBase, 'trade_cal', getTrade_cal(begin,end))
    print('trade_cal已存入数据库.')
    
# 每日停复牌表    
def writeSuspend_d(dataBase, begin, end):
    # 设定开始和结束日期
    start_date = dt.datetime.strptime(begin,'%Y%m%d')
    end_date = dt.datetime.strptime(end,'%Y%m%d')
    # 循环遍历每一天
    for single_date in range((end_date - start_date).days + 1):
        #先判断这一天是否是交易日
        current_date = start_date + dt.timedelta(days=single_date)
        if isTradingDay(dataBase,current_date): 
            if dataBase.has_symbol('suspend_d'):
                dataBase.append('suspend_d', getSuspend_d(current_date.strftime('%Y%m%d')))
                print('追加数据表suspend_d'+current_date.strftime('%Y%m%d')+'已存入数据库.')
            else:
                writeDB(dataBase,'suspend_d', getSuspend_d(current_date.strftime('%Y%m%d')))
                print('开始创建suspend_d'+current_date.strftime('%Y%m%d')+'已存入数据库.')

# 每日涨跌停表,暂时没用到这张表    
def writeStk_limit(dataBase, begin, end):
    start_date = dt.datetime.strptime(begin,'%Y%m%d')
    end_date = dt.datetime.strptime(end,'%Y%m%d')
    # 循环遍历每一天
    for single_date in range((end_date - start_date).days + 1):
        #先判断这一天是否是交易日
        current_date = start_date + dt.timedelta(days=single_date)
        if isTradingDay(dataBase,current_date): 
            if dataBase.has_symbol('stk_limit'):
                dataBase.append('stk_limit', getStk_limit(current_date.strftime('%Y%m%d')))
                print('追加数据表stk_limit'+current_date.strftime('%Y%m%d')+'已存入数据库.')
            else:
                writeDB(dataBase,'stk_limit', getStk_limit(current_date.strftime('%Y%m%d')))
                print('开始创建stk_limit'+current_date.strftime('%Y%m%d')+'已存入数据库.')
    
#将从begin到end的Daily_basic数据表存入到数据库
def writeDaily_basic(dataBase, begin: str, end: str):
    # 设定开始和结束日期
    start_date = dt.datetime.strptime(begin,'%Y%m%d')
    end_date = dt.datetime.strptime(end,'%Y%m%d')
    # 循环遍历每一天
    for single_date in range((end_date - start_date).days + 1):
        #先判断这一天是否是交易日
        current_date = start_date + dt.timedelta(days=single_date)
        if isTradingDay(dataBase,current_date): 
            if dataBase.has_symbol('daily_basic'):
                df = getDaily_basic(current_date.strftime('%Y%m%d'))
                df['trade_date'] = df['trade_date'].apply(lambda x:dt.datetime.strptime(x,'%Y%m%d')).values
                df.set_index(['trade_date'],inplace=True)
                dataBase.append('daily_basic', df)
                print('追加数据表daily_basic'+current_date.strftime('%Y%m%d')+'已存入数据库.')
            else:
                df = getDaily_basic(current_date.strftime('%Y%m%d'))
                df['trade_date'] = df['trade_date'].apply(lambda x:dt.datetime.strptime(x,'%Y%m%d')).values
                df.set_index(['trade_date'],inplace=True)
                writeDB(dataBase,'daily_basic', df)
                print('开始创建daily_basic'+current_date.strftime('%Y%m%d')+'已存入数据库.')

#将从begin到end的Pro_bar数据表存入到数据库
def writePro_bar(dataBase, begin: str, end: str):
    # 设定开始和结束日期
    start_date = dt.datetime.strptime(begin,'%Y%m%d')
    end_date = dt.datetime.strptime(end,'%Y%m%d')
    # 循环遍历每一天
    for single_date in range((end_date - start_date).days + 1):
        #先判断这一天是否是交易日
        current_date = start_date + dt.timedelta(days=single_date)
        if isTradingDay(dataBase,current_date): 
            if dataBase.has_symbol('daily_basic'):
                dataBase.append('daily_basic', getDaily_basic(current_date.strftime('%Y%m%d')))
                print('追加数据表daily_basic'+current_date.strftime('%Y%m%d')+'已存入数据库.')
            else:
                writeDB(dataBase,'daily_basic', getDaily_basic(current_date.strftime('%Y%m%d')))
                print('开始创建daily_basic'+current_date.strftime('%Y%m%d')+'已存入数据库.')        

#在本地建立数据库文件,初始化文件大小为40G,是的,即便是日频数据量也很大...
# ac = adb.Arctic('lmdb://./data/IFactorDB/database??map_size=20GB')
# begin = '20050101'#Backtest from 2005-01-01
# end = '20240424'#Backtest to 2024-04-24
# library = ac['tsData']    
    
# df = getPro_bar('600519.SH','20050101','20181011')
# writeTrade_cal(library, begin)
# writeDaily_basic(library, begin, end)
# writeSuspend_d(library, begin, end)
# stocklist = getStockPool(library, '20150212', 0)
# library.delete('daily_basic')
# library.list_symbols()
# q = QueryBuilder()
# q = q[(q["trade_date"] == '20240220') ]
# # from_storage_df = library.read('daily_basic',query_builder=q).data
# from_storage_df = library.read('daily_basic').data
# print(from_storage_df)
