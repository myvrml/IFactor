'''
Author: Van Sun
Date: 2024-04-23 11:37:31
LastEditTime: 2024-04-24 09:44:41
LastEditors: Van Sun
Description: 
FilePath: \IFactor\getTushareData.py

'''
import tushare as ts
from os import environ  # environ 用于获取系统环境变量
from dotenv import load_dotenv  # load_dotenv 用于将 .env 文件的内容加载到系统环境变量中

def getTestDF():
    load_dotenv()  # 从程序所在目录的 .env 文件中加载环境变量
    #ts.set_token(environ.get("TUSHARE_TOKEN"))
    pro = ts.pro_api()
    df = pro.query('trade_cal', exchange='', start_date='20180901', end_date='20181001', 
                fields='exchange,cal_date,is_open,pretrade_date', is_open='0')
    return df

import arcticdb as adb

print(getTestDF())

