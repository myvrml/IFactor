'''
Author: Van Sun
Date: 2024-05-26 12:08:45
LastEditTime: 2024-05-30 17:34:58
LastEditors: Van Sun
Description: 
FilePath: \IFactor\test_Dolphin.py

'''
import dolphindb as ddb
import datetime as dt
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
s.run("delete from loadTable('{db}','{tb}') where ts_code like '4%'"\
    .format(db="dfs://k_day_level", tb="company_detail"))
s.run("delete from loadTable('{db}','{tb}') where ts_code like '4%'"\
    .format(db="dfs://dayFactorDB", tb="factor_basic"))
# tb = s.table(dbPath="dfs://dayFactorDB", data="factor_computed")
# date=dt.datetime(year=2017,month=12,day=29)
# from_storage_df = tb.select("*").where(f"trade_date={date.strftime('%Y.%m.%d')}").toDF()

tb = s.table(dbPath="dfs://dayFactorDB", data="factor_basic")
date=dt.datetime(year=2017,month=12,day=29)
from_storage_df = tb.select("*").where("ts_code LIKE '4%'").toDF()
print(from_storage_df)
s.close()