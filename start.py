'''
Author: Van Sun
Date: 2024-04-23 18:27:50
LastEditTime: 2024-04-23 20:17:15
LastEditors: Van Sun
Description: 
FilePath: \IFactor\start.py

'''
import lmdb
# 创建并打开数据库
dataBase = lmdb.open("./data/IFactorDB", map_size=2147483648) #2GB 文件型数据库
# 获取数据库句柄
db = dataBase.open_db()
