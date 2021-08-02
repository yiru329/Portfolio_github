# _*_ coding:utf-8   _*_
# 导入函数库
from jqdata import *

import pandas as pd
import datetime
from six import BytesIO

__author__ = 'yara'

'''聚宽平台
单均线策略; 2001-08-27(2005-01-05） 到 2020-06-11, ￥100000, 每天; 
买点：价格自上而下跌破均线，后自下而上突破时
卖点：下一次自上而下跌破均线（赚：止盈出场，亏：止损）

注：单均线代码需要变换'''

'''
================================================================================
总体回测前
================================================================================
'''


# 总体回测前要做的事情；初始化函数，设定要操作的股票、基准等等；  平台只有2005年后的数据
def initialize(context):
    # g.tc = 15  # 调仓频率
    g.N = 1  # 持仓数目
    g.security = ["600519.XSHG"]  # 全局变量，设置股票池

    # set_benchmark('000300.XSHG')  # 设定沪深300作为基准； 上海证券交易所XSHG   深圳证券交易所XSHE
    set_benchmark('600519.XSHG')

    set_option('use_real_price', True)  # 开启动态复权模式(真实价格)

    # # 运行函数
    # handle_data(context, data)

    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')


'''
================================================================================
每天开盘前
================================================================================
'''


# 每天开盘前要做的事情
def before_trading_start(context):
    # 输出运行时间
    log.info('函数运行时间(before_market_open)：' + str(context.current_dt.time()))

    # 给微信发送消息（添加模拟交易，并绑定微信生效）
    # send_message('美好的一天~')

    set_slip_fee(context)


# 4
# 根据不同的时间段设置滑点与手续费
def set_slip_fee(context):
    # 将滑点设置为0
    set_slippage(FixedSlippage(0))
    # 根据不同的时间段设置手续费
    dt = context.current_dt

    if dt > datetime.datetime(2013, 1, 1):
        set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))

    elif dt > datetime.datetime(2011, 1, 1):
        set_commission(PerTrade(buy_cost=0.001, sell_cost=0.002, min_cost=5))

    elif dt > datetime.datetime(2009, 1, 1):
        set_commission(PerTrade(buy_cost=0.002, sell_cost=0.003, min_cost=5))

    else:
        set_commission(PerTrade(buy_cost=0.003, sell_cost=0.004, min_cost=5))


'''
================================================================================
每天交易时
================================================================================
'''


def handle_data(context, data):
    log.info('函数运行时间(market_open):'+str(context.current_dt.time()))

    # 将总资金等分为g.N份，为每只股票配资
    capital_unit = context.portfolio.portfolio_value / g.N

    #获取本地数据(日期为索引，并格式化）：先上传到网页“研究中心'  ;乘以复权因子，算出前复权价格
    body = read_file("600519_price_f.csv")
    local_data = pd.read_csv(BytesIO(body))
    # local_data = pd.read_csv('600519_price_f.csv')
    # 将date时间格式化'2019-01-01',转换为没有时分秒的日期
    local_data['date'] = pd.to_datetime(local_data['date'], format="%Y-%m-%d")
    local_data['date'] = local_data['date'].dt.date
    local_data = local_data.set_index('date')

    #复权，改名
    local_data['bps_stock_f'] = local_data['bps_stock'] * local_data['f_factor']
    local_data['dividend_stock_f'] = local_data['dividend_stock'] * local_data['f_factor']
    local_data['Intrinsic_Value_Projected_FCF_f'] = local_data['Intrinsic Value: Projected FCF'] * local_data[
        'f_factor']
    local_data['Median_PS_Value_f'] = local_data['Median PS Value'] * local_data['f_factor']
    local_data['Peter_Lynch_Fair_Value_f'] = local_data['Peter Lynch Fair Value'] * local_data['f_factor']
    local_data['Graham_Number_f'] = local_data['Graham Number'] * local_data['f_factor']
    local_data['EPV_f'] = local_data['Earnings Power Value (EPV)'] * local_data['f_factor']

    # #不复权，改名
    # local_data['bps_stock_f'] = local_data['bps_stock']
    # local_data['dividend_stock_f'] = local_data['dividend_stock']
    # local_data['Intrinsic_Value_Projected_FCF_f'] = local_data['Intrinsic Value: Projected FCF']
    # local_data['Median_PS_Value_f'] = local_data['Median PS Value']
    # local_data['Peter_Lynch_Fair_Value_f'] = local_data['Peter Lynch Fair Value']
    # local_data['Graham_Number_f'] = local_data['Graham Number']
    # local_data['EPV_f'] = local_data['Earnings Power Value (EPV)']

    toSell = signal_stock_sell(context, data, local_data, 'bps_stock_f')
    toBuy = signal_stock_buy(context, data, local_data, 'bps_stock_f')

    # toSell = signal_stock_sell(context, data, local_data, 'dividend_stock_f')
    # toBuy = signal_stock_buy(context, data, local_data, 'dividend_stock_f')
    #
    # toSell = signal_stock_sell(context, data, local_data, 'Intrinsic_Value_Projected_FCF_f')
    # toBuy = signal_stock_buy(context, data, local_data, 'Intrinsic_Value_Projected_FCF_f')
    #
    # toSell = signal_stock_sell(context, data, local_data, 'Median_PS_Value_f')
    # toBuy = signal_stock_buy(context, data, local_data, 'Median_PS_Value_f')
    #
    # toSell = signal_stock_sell(context, data, local_data, 'Peter_Lynch_Fair_Value_f')
    # toBuy = signal_stock_buy(context, data, local_data, 'Peter_Lynch_Fair_Value_f')
    #
    # toSell = signal_stock_sell(context, data, local_data, 'Graham_Number_f')
    # toBuy = signal_stock_buy(context, data, local_data, 'Graham_Number_f')
    #
    # toSell = signal_stock_sell(context, data, local_data, 'EPV_f')
    # toBuy = signal_stock_buy(context, data, local_data, 'EPV_f')

    # 执行卖出操作以腾出资金
    for i in range(len(g.security)):
        if toSell[i] == 1:
            order_target_value(g.security[i], 0)
    # 执行买入操作
    for i in range(len(g.security)):
        if toBuy[i] == 1:
            order_target_value(g.security[i], capital_unit)
    if not (1 in toBuy) or (1 in toSell):
        # log.info("今日无操作")
        send_message("今日无操作")


# 5
# 获得卖出信号
# 输入：context, data
# 输出：sell - list
def signal_stock_sell(context, data, local_data, mean_index:str):
    sell = [0] * len(g.security)
    for i in range(len(g.security)):
        # # 算出今天和昨天的两个指数移动均线的值，我们这里假设长线是60天，短线是1天(前一天的收盘价)
        # (ema_long_pre, ema_long_now) = get_EMA(g.security[i], 60, data)
        # (ema_short_pre, ema_short_now) = get_EMA(g.security[i], 1, data)
        # # 如果短均线从上往下穿越长均线，则为死叉信号，标记卖出
        # if ema_short_now < ema_long_now and ema_short_pre > ema_long_pre and context.portfolio.positions[
        #     g.security[i]].sellable_amount > 0:
        #     sell[i] = 1

        # 如果股价从上往下穿越均线，则为死叉信号，标记卖出
        api_data = attribute_history(g.security[i], 2, unit='1d',fields=['close'],
                          skip_paused=True, df=True, fq='pre')
        all_data = api_data.join(local_data)

        price_f_pre = all_data['close'][0]
        price_f_now = all_data['close'][1]
        mean_price_f_pre = all_data['{}'.format(mean_index)][0]
        mean_price_f_now = all_data['{}'.format(mean_index)][1]
        if price_f_now < mean_price_f_now and price_f_pre > mean_price_f_pre and context.portfolio.positions[
            g.security[i]].sellable_amount > 0:
            sell[i] = 1

    return sell


# 6
# 获得买入信号
# 输入：context, data
# 输出：buy - list
def signal_stock_buy(context, data, local_data, mean_index:str):
    buy = [0] * len(g.security)
    for i in range(len(g.security)):
        # # 算出今天和昨天的两个指数移动均线的值，我们这里假设长线是60天，短线是1天(前一天的收盘价)
        # (ema_long_pre, ema_long_now) = get_EMA(g.security[i], 60, data)
        # (ema_short_pre, ema_short_now) = get_EMA(g.security[i], 1, data)
        # # 如果短均线从下往上穿越长均线，则为金叉信号，标记买入
        # if ema_short_now > ema_long_now and ema_short_pre < ema_long_pre and context.portfolio.positions[
        #     g.security[i]].sellable_amount == 0:
        #     buy[i] = 1

        # # 得到当前时间
        # today = context.current_dt
        # # 得到该股票上一时间点价格
        # current_price = hist[security][0]

        # 选取一只股票；   价格自上而下跌破均线，后自下而上突破时，则为金叉信号，标记买入
        api_data = attribute_history(g.security[i], 2, unit='1d',fields=['close'],
                          skip_paused=True, df=True, fq='pre')
        all_data = api_data.join(local_data)

        price_f_pre = all_data['close'][0]
        price_f_now = all_data['close'][1]
        mean_price_f_pre = all_data['{}'.format(mean_index)][0]
        mean_price_f_now = all_data['{}'.format(mean_index)][1]
        if price_f_now > mean_price_f_now and price_f_pre < mean_price_f_pre and context.portfolio.positions[
            g.security[i]].sellable_amount == 0:
            buy[i] = 1

    return buy


'''
================================================================================
每天收盘后
================================================================================
'''


# 每日收盘后要做的事情（本策略中不需要）
def after_trading_end(context):
    return






