'''
Author: Van Sun
Date: 2024-04-23 11:38:02
LastEditTime: 2024-04-23 20:57:23
LastEditors: Van Sun
Description: 
FilePath: \IFactor\saveDataToArcticDB.py

'''
import arcticdb as adb
from getTushareData import *
ac = adb.Arctic('lmdb://./data/IFactorDB/database?map_size=2GB')
def initDB():
    ac.create_library('tsData')
    
ac.list_libraries()
library = ac['tsData']

def writeDB():
    library.write('test_frame', getTestDF())
    
from_storage_df = library.read('test_frame').data
print(from_storage_df)