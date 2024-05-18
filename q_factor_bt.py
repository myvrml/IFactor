'''
Author: Van Sun
Date: 2024-05-11 18:15:36
LastEditTime: 2024-05-18 17:23:30
LastEditors: Van Sun
Description: back testing with Backtrader
FilePath: \IFactor\q_factor_bt.py

'''
import arcticdb as adb
import numpy as np
from sklearn.preprocessing import StandardScaler
from arcticdb import QueryBuilder
from getTushareData import *
import datetime 
from dateutil.relativedelta import *
from mytools import *
from saveDataToArcticDB import writeDB
import backtrader as bt # 导入 Backtrader
import backtrader.indicators as btind # 导入策略分析模块
import backtrader.feeds as btfeeds # 导入数据模块
ac = adb.Arctic('lmdb://./data/IFactorDB/database?map_size=50GB')
library = ac['tsData'] 
pro = ts.pro_api()  
# 实例化 cerebro
cerebro = bt.Cerebro(stdstats=False)
#取股价
begin = datetime.datetime(2024, 3, 29)#Backtest from 2010-01-01
end = datetime.datetime(2024, 4, 24)#Backtest end 2024-04-24
daily_price = get_stock_price_data(library, begin,end)
# daily_price = []
# trade_info = pd.read_csv("result.csv", parse_dates=['date'])

def get_all_rebalance_days(rebalance_day, begin,end):
    tmplist_rebalance_days = []
    list_rebalance_days = []
    if begin.day <= rebalance_day:
        first_rebalance_day = datetime.datetime(begin.year, begin.month, rebalance_day)
    else:
        first_rebalance_day = datetime.datetime(begin.year, (begin.month+1), rebalance_day)
    if end.day >= rebalance_day:
        last_rebalance_day = datetime.datetime(end.year, end.month, rebalance_day)
    else:
        last_rebalance_day = datetime.datetime(end.year, (end.month-1), rebalance_day)
    year_diff = relativedelta(last_rebalance_day, first_rebalance_day).years
    months_diff = relativedelta(last_rebalance_day, first_rebalance_day).months
    months_diff = year_diff * 12 + months_diff +1
    for single_month in range(months_diff):
        tmplist_rebalance_days.append(first_rebalance_day+relativedelta(months=single_month))
    for day in tmplist_rebalance_days:
        if isTradingDay(library,day):
            list_rebalance_days.append(day)
        else:
            while day <= end:
               day += datetime.timedelta(days=1)
               if isTradingDay(library,day):
                   list_rebalance_days.append(day)
                   break
    del tmplist_rebalance_days
    return list_rebalance_days
# aa=get_all_rebalance_days(8,begin,end)
# 按股票代码，依次循环传入数据
for stock in daily_price['ts_code'].unique():
    # 日期对齐
    data = pd.DataFrame(daily_price['trade_date'].unique(), columns=['trade_date'])  # 获取回测区间内所有交易日
    df = daily_price.query(f"ts_code=='{stock}'")[
        ['trade_date', 'open', 'high', 'low', 'close', 'vol']]
    df['openinterest'] = 0
    data_ = pd.merge(data, df, how='left', on='trade_date')
    data_.rename(columns={'trade_date': 'datetime', 'ts_code': 'sec_code', 'vol': 'volume'}, inplace=True)
    data_ = data_.set_index("datetime")
    # print(data_.dtypes)
    # 缺失值处理：日期对齐时会使得有些交易日的数据为空，所以需要对缺失数据进行填充
    # data_.loc[:, ['volume', 'openinterest']] = data_.loc[:, ['volume', 'openinterest']].fillna(0)
    data_.loc[:, ['open', 'high', 'low', 'close']] = data_.loc[:, ['open', 'high', 'low', 'close']].fillna(method='pad')
    data_.loc[:, ['open', 'high', 'low', 'close']] = data_.loc[:, ['open', 'high', 'low', 'close']].fillna(0)
    # 导入数据
    datafeed = bt.feeds.PandasData(dataname=data_, fromdate=begin,
                                   todate=end)
    cerebro.adddata(datafeed, name=stock)  # 通过 name 实现数据集与股票的一一对应
    # print(f"{stock} Done !")

print("All stock Done !")

# 回测策略
class TestStrategy(bt.Strategy):
    '''选股策略'''
    params = (('maperiod', 15),
              ('printlog', False),)

    def __init__(self):
        #开始计算所有的调仓日期:每月的rebalance_day号,如果未开市则后延到下一个交易日
        #回测时，会在这一天下单，然后在下一个交易日，以开盘价买入
        self.trade_dates = get_all_rebalance_days(21, begin=begin, end=end)
        s_df = pd.DataFrame()
        for day in self.trade_dates:
            factor_df = compute_multifactor_data(library,'total_mv,pb',\
                'q_factor_investment',day,day)
            #对factor取quantile_num组,然后获得最高一组的所有股票代码
            quantile_num = 20
            df = factor_df[(factor_df['compounded_q_factor']>=\
                factor_df['compounded_q_factor'].quantile(1-1/quantile_num))]
            df['weight'] = 1/len(df)
            # df.loc[:, ['weight']] = df.loc[:, ['weight']].fillna(1/len(df))
            s_df = pd.concat([s_df, df], axis=0)
        s_df.rename(columns={'trade_date': 'trade_date', 'ts_code': 'sec_code'}, inplace=True)
        self.buy_stock = s_df[['trade_date','sec_code','weight']]  # 保留调仓列表
        self.order_list = []  # 记录以往订单，方便调仓日对未完成订单做处理
        self.buy_stocks_pre = []  # 记录上一期持仓

    def next(self):
        dt = self.datas[0].datetime.datetime(0)  # 获取当前的回测时间点
        # 如果是调仓日，则进行调仓操作 dt是date,而trade_dates是datetime,需转化
        if dt in self.trade_dates:
            print("--------------{} 为调仓日----------".format(dt))
            # 在调仓之前，取消之前所下的没成交也未到期的订单
            if len(self.order_list) > 0:
                for od in self.order_list:
                    self.cancel(od)  # 如果订单未完成，则撤销订单
                self.order_list = []  # 重置订单列表
            # 提取当前调仓日的持仓列表
            buy_stocks_data = self.buy_stock.query(f"trade_date=='{dt}'")
            long_list = buy_stocks_data['sec_code'].tolist()
            buy_stocks_data.to_csv('I3Factor_position.csv')
            # print('long_list', long_list)  # 打印持仓列表
            # 对现有持仓中，调仓后不再继续持有的股票进行卖出平仓
            sell_stock = [i for i in self.buy_stocks_pre if i not in long_list]
            # print('sell_stock', sell_stock)  # 打印平仓列表
            if len(sell_stock) > 0:
                print("-----------对不再持有的股票进行平仓--------------")
                for stock in sell_stock:
                    data = self.getdatabyname(stock)
                    if self.getposition(data).size > 0:
                        od = self.close(data=data)
                        self.order_list.append(od)  # 记录卖出订单
            # 买入此次调仓的股票：多退少补原则
            print("-----------买入此次调仓期的股票--------------")
            for stock in long_list:
                w = buy_stocks_data.query(f"sec_code=='{stock}'")['weight'].iloc[0]  # 提取持仓权重
                data = self.getdatabyname(stock)
                order = self.order_target_percent(data=data, target=w * 0.95)  # 为减少可用资金不足的情况，留 5% 的现金做备用
                self.order_list.append(order)

            self.buy_stocks_pre = long_list  # 保存此次调仓的股票列表

        # 交易记录日志（可省略，默认不输出结果）

    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()},{txt}')

    def notify_order(self, order):
        # 未被处理的订单
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 已经处理的订单
        if order.status in [order.Completed, order.Canceled, order.Margin]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, ref:%.0f，Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s' %
                    (order.ref,  # 订单编号
                     order.executed.price,  # 成交价
                     order.executed.value,  # 成交额
                     order.executed.comm,  # 佣金
                     order.executed.size,  # 成交量
                     order.data._name))  # 股票名称
            else:  # Sell
                self.log('SELL EXECUTED, ref:%.0f, Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s' %
                         (order.ref,
                          order.executed.price,
                          order.executed.value,
                          order.executed.comm,
                          order.executed.size,
                          order.data._name))


# 初始资金 10,000,000
cerebro.broker.setcash(10000000.0)
# 佣金，双边各 0.001
cerebro.broker.setcommission(commission=0.001)
# 滑点：双边各 0.0005
cerebro.broker.set_slippage_perc(perc=0.001)

cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='pnl')  # 返回收益率时序数据
# cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')  # 年化收益率
# cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio')  # 夏普比率
# cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')  # 回撤

# 将编写的策略添加给大脑，别忘了 ！
cerebro.addstrategy(TestStrategy, printlog=True)

# 启动回测
result = cerebro.run()
# 从返回的 result 中提取回测结果
strat = result[0]
print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
# 返回日度收益率序列
daily_return = pd.Series(strat.analyzers.pnl.get_analysis())
daily_return.to_csv('dailyReturn.csv')
# 打印评价指标
# print("--------------- AnnualReturn -----------------")
# print(strat.analyzers._AnnualReturn.get_analysis())
# print("--------------- SharpeRatio -----------------")
# print(strat.analyzers._SharpeRatio.get_analysis())
# print("--------------- DrawDown -----------------")
# print(strat.analyzers._DrawDown.get_analysis())


# cerebro.plot()