import arcticdb as adb
import datetime as dt

ac = adb.Arctic('lmdb://./data/IFactorDB/database?map_size=20GB')
    
begin = '20171201'#2017年之前的tushare数据质量不佳
end = '20240523'#
library = ac.get_library('tsData', create_if_missing=True)
from_storage_df = library.read('suspend_d').data
from_storage_df['trade_date'] = from_storage_df['trade_date'].apply(lambda x:dt.datetime.strptime(x,'%Y%m%d')).values
# from_storage_df['pretrade_date'] = from_storage_df['pretrade_date'].apply(lambda x:dt.datetime.strptime(x,'%Y%m%d')).values
print(from_storage_df.head())

import dolphindb as ddb
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
appender = ddb.TableAppender(dbPath="dfs://k_day_level",tableName="suspend_d", ddbSession=s)
# num = appender.append(from_storage_df[['ts_code','trade_date', 'open', 'high', 'low', 'close','vol']])
# print("append rows: ", num)
num = appender.append(from_storage_df)
print("append rows: ", num)
pt = s.loadTable("suspend_d", dbPath="dfs://k_day_level")
print(pt, pt.tableName())
print(pt.toDF())
# t = s.run("k_day")
# print(t)
s.close()
# schema = s.run("schema(k_day)")
# print(schema["colDefs"])