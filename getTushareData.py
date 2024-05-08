'''
Author: Van Sun
Date: 2024-04-23 11:37:31
LastEditTime: 2024-04-25 10:36:47
LastEditors: Van Sun
Description: 
FilePath: \IFactor\getTushareData.py

'''
import tushare as ts
from os import environ  # environ 用于获取系统环境变量
from dotenv import load_dotenv  # load_dotenv 用于将 .env 文件的内容加载到系统环境变量中
import arcticdb as adb
def getTestDF():
    load_dotenv()  # 从程序所在目录的 .env 文件中加载环境变量
    
    pro = ts.pro_api()
    df = pro.query('trade_cal', exchange='', start_date='20180901', end_date='20181001', 
                fields='exchange,cal_date,is_open,pretrade_date', is_open='0')
    return df



""" 接口：stock_basic，可以通过数据工具调试和查看数据
描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
积分：2000积分起

输入参数

名称	类型	必选	描述
ts_code	str	N	TS股票代码
name	str	N	名称
market	str	N	市场类别 （主板/创业板/科创板/CDR/北交所）
list_status	str	N	上市状态 L上市 D退市 P暂停上市，默认是L
exchange	str	N	交易所 SSE上交所 SZSE深交所 BSE北交所
is_hs	str	N	是否沪深港通标的，N否 H沪股通 S深股通
输出参数

名称	类型	默认显示	描述
ts_code	str	Y	TS代码
symbol	str	Y	股票代码
name	str	Y	股票名称
area	str	Y	地域
industry	str	Y	所属行业
fullname	str	N	股票全称
enname	str	N	英文全称
cnspell	str	Y	拼音缩写
market	str	Y	市场类型（主板/创业板/科创板/CDR）
exchange	str	N	交易所代码
curr_type	str	N	交易货币
list_status	str	N	上市状态 L上市 D退市 P暂停上市
list_date	str	Y	上市日期
delist_date	str	N	退市日期
is_hs	str	N	是否沪深港通标的，N否 H沪股通 S深股通
act_name	str	Y	实控人名称
act_ent_type	str	Y	实控人企业性质 """
def getStock_basic():
    pro = ts.pro_api()
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    return df

""" 交易日期表
接口：trade_cal，可以通过数据工具调试和查看数据。
描述：获取各大交易所交易日历数据,默认提取的是上交所
积分：需2000积分

输入参数

名称	类型	必选	描述
exchange	str	N	交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源
start_date	str	N	开始日期 （格式：YYYYMMDD 下同）
end_date	str	N	结束日期
is_open	str	N	是否交易 '0'休市 '1'交易
输出参数

名称	类型	默认显示	描述
exchange	str	Y	交易所 SSE上交所 SZSE深交所
cal_date	str	Y	日历日期
is_open	str	Y	是否交易 0休市 1交易
pretrade_date	str	Y	上一个交易日 """
def getTrade_cal(begin: str, end: str):
    pro = ts.pro_api()
    df = pro.query('trade_cal', start_date=begin, end_date=end)
    return df

""" 每日停复牌表
接口：suspend_d
更新时间：不定期
描述：按日期方式获取股票每日停复牌信息

输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码(可输入多值)
trade_date	str	N	交易日日期
start_date	str	N	停复牌查询开始日期
end_date	str	N	停复牌查询结束日期
suspend_type	str	N	停复牌类型：S-停牌,R-复牌
输出参数

名称	类型	默认显示	描述
ts_code	str	Y	TS代码
trade_date	str	Y	停复牌日期
suspend_timing	str	Y	日内停牌时间段
suspend_type	str	Y	停复牌类型：S-停牌，R-复牌 """
def getSuspend_d(tradingday):
    pro = ts.pro_api()
    df = pro.query('suspend_d', trade_date=tradingday, fields='ts_code,trade_date,suspend_type')
    return df


def getStk_limit(tradingday):
    pro = ts.pro_api()
    df = pro.query('stk_limit', trade_date=tradingday)
    return df
""" 每日指标表,可以通过此表取到某一交易日的所有股票代码和基本面指标
接口：daily_basic，可以通过数据工具调试和查看数据。
更新时间：交易日每日15点～17点之间
描述：获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。
积分：至少2000积分才可以调取，5000积分无总量限制，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
ts_code	str	Y	股票代码（二选一）
trade_date	str	N	交易日期 （二选一）
start_date	str	N	开始日期(YYYYMMDD)
end_date	str	N	结束日期(YYYYMMDD)
注：日期都填YYYYMMDD格式，比如20181010

输出参数

名称	类型	描述
ts_code	str	TS股票代码
trade_date	str	交易日期
close	float	当日收盘价
turnover_rate	float	换手率（%）
turnover_rate_f	float	换手率（自由流通股）
volume_ratio	float	量比
pe	float	市盈率（总市值/净利润， 亏损的PE为空）
pe_ttm	float	市盈率（TTM，亏损的PE为空）
pb	float	市净率（总市值/净资产）
ps	float	市销率
ps_ttm	float	市销率（TTM）
dv_ratio	float	股息率 （%）
dv_ttm	float	股息率（TTM）（%）
total_share	float	总股本 （万股）
float_share	float	流通股本 （万股）
free_share	float	自由流通股本 （万）
total_mv	float	总市值 （万元）
circ_mv	float	流通市值（万元） """
def getDaily_basic(tradeDay: str):
    pro = ts.pro_api()
    df = pro.query('daily_basic', ts_code='', trade_date=tradeDay,
                   fields='''ts_code,trade_date,close,turnover_rate,volume_ratio,pe,pe_ttm,pb,ps,
                   ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv''')
    return df

""" 日频个股交易表(高开低收,涨跌幅等)
接口名称：pro_bar，可以通过数据工具调试和查看数据。
更新时间：股票和指数通常在15点～17点之间，数字货币实时更新，具体请参考各接口文档明细。
描述：目前整合了股票（未复权、前复权、后复权）、指数、数字货币、ETF基金、期货、期权的行情数据，未来还将整合包括外汇在内的所有交易行情数据，同时提供分钟数据。不同数据对应不同的积分要求，具体请参阅每类数据的文档说明。
其它：由于本接口是集成接口，在SDK层做了一些逻辑处理，目前暂时没法用http的方式调取通用行情接口。用户可以访问Tushare的Github，查看源代码完成类似功能。

输入参数

名称	类型	必选	描述
ts_code	str	Y	证券代码，不支持多值输入，多值输入获取结果会有重复记录
start_date	str	N	开始日期 (日线格式：YYYYMMDD，提取分钟数据请用2019-09-01 09:00:00这种格式)
end_date	str	N	结束日期 (日线格式：YYYYMMDD)
asset	str	Y	资产类别：E股票 I沪深指数 C数字货币 FT期货 FD基金 O期权 CB可转债（v1.2.39），默认E
adj	str	N	复权类型(只针对股票)：None未复权 qfq前复权 hfq后复权 , 默认None，目前只支持日线复权，同时复权机制是根据设定的end_date参数动态复权，采用分红再投模式，具体请参考常见问题列表里的说明，如果获取跟行情软件一致的复权行情，可以参阅股票技术因子接口。
freq	str	Y	数据频度 ：支持分钟(min)/日(D)/周(W)/月(M)K线，其中1min表示1分钟（类推1/5/15/30/60分钟） ，默认D。对于分钟数据有600积分用户可以试用（请求2次），正式权限可以参考权限列表说明 ，使用方法请参考股票分钟使用方法。
ma	list	N	均线，支持任意合理int数值。注：均线是动态计算，要设置一定时间范围才能获得相应的均线，比如5日均线，开始和结束日期参数跨度必须要超过5日。目前只支持单一个股票提取均线，即需要输入ts_code参数。e.g: ma_5表示5日均价，ma_v_5表示5日均量
factors	list	N	股票因子（asset='E'有效）支持 tor换手率 vr量比
adjfactor	str	N	复权因子，在复权数据时，如果此参数为True，返回的数据中则带复权因子，默认为False。 该功能从1.2.33版本开始生效 
输出参数

名称	类型	描述
ts_code	str	股票代码
trade_date	str	交易日期
open	float	开盘价
high	float	最高价
low	float	最低价
close	float	收盘价
pre_close	float	昨收价(前复权)
change	float	涨跌额
pct_chg	float	涨跌幅 
vol	float	成交量 （手）
amount	float	成交额 （千元）"""
def getPro_bar(tsCode:str, begin: str, end: str):
    df = ts.pro_bar(ts_code=tsCode, asset='E', adj='qfq',adjfactor=True, start_date=begin, end_date=end, factors=['tor', 'vr'])
    print(df[['close','pre_close','pct_chg']])
    return df

