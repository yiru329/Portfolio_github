# _*_ coding:utf-8   _*_
# 导入函数库
# from jqdata import *
# from six import BytesIO

import pandas as pd
import numpy as np
import datetime

__author__ = 'yara'

'''
单均线策略; 逐行计算,
初始资金：股票第一天不复权的价格, 每天; 
buy点：价格自上而下跌破均线，后自下而上突破时
卖点：下一次自上而下跌破均线（赚：Stopwin出场， 亏：是否小于StopLoss点？小于，出场）

注：单均线指标名称 需要变换
指数运算用**
'''


"""
注：带“_r”的都是比率，无单位，小数        此策略计算的是每个买卖区间内的收益结果

需要写入csv的指标（buy或卖）：
date 日期，TradeType 交易类型（buy，Stopwin，StopLoss），nTradePrice 成交价， 
nExpectedStopLoss_r 预期停损比率（buy入价 nEntryPrice，StopLoss(nLowestUnder)点 nStopLoss），

nTradeProfit_r 策略收益（前复权价格期初 nEntryPrice、期末价值 nExitPrice），
nTradeProfitAnaly_r 策略年化收益（策略收益，策略执行工作日天数 nDaysOfTrade），
nTradeProfitVolatility_r 策略收益波动率(nTradeProfitDay_r 策略每日收益率（nClose - nPreClose）/ nPreClose, 及其标准差， 策略执行工作日天数 nDaysOfTrade),
nSharpe_r 夏普比率（nTradeProfitAnaly_r 策略年化收益，无风险利率0.04，nTradeProfitVolatility_r 策略收益波动率），
nWin_r 胜率（盈利交易次数 nWinTrade，总交易次数 nTradeCnt），
nGainLoss_r 盈亏比（总盈利额 nProfit，总亏损额 nLoss），
nMaxDrawdown_r 最大回撤（持有股票账户价值最高点 nhighestUp、最低点 nLowestUnder），nMaxDrawdown 最大回撤金额
nCapital 账户金额
"""

def FairPrice_Strategy(code: str, fair_value: str):
    # 获取本地数据(日期为索引，并格式化）：先上传到网页“研究中心 #  ;乘以复权因子，算出前复权价格
    local_data = pd.read_csv('{}.csv'.format(code))
    # 将date时间格式化'2019-01-01',转换为没有时分秒的日期
    local_data['date'] = pd.to_datetime(local_data['date'], format="%Y-%m-%d")
    local_data['date'] = local_data['date'].dt.date
    # 将时间设置为索引
    # local_data = local_data.set_index('date')

    # 复权，改名
    local_data['bps_stock_f'] = local_data['bps_stock'] * local_data['f_factor']
    local_data['dividend_stock_f'] = local_data['dividend_stock'] * local_data['f_factor']
    local_data['Intrinsic_Value_Projected_FCF_f'] = local_data['Intrinsic Value: Projected FCF'] * local_data[
        'f_factor']
    local_data['Median_PS_Value_f'] = local_data['Median PS Value'] * local_data['f_factor']
    local_data['Peter_Lynch_Fair_Value_f'] = local_data['Peter Lynch Fair Value'] * local_data['f_factor']
    local_data['Graham_Number_f'] = local_data['Graham Number'] * local_data['f_factor']
    local_data['EPV_f'] = local_data['Earnings Power Value (EPV)'] * local_data['f_factor']

    # 初始值
    nOI = 0  # 是否持仓（否0，是1）
    nhighestUp = 0  # 最高点
    nLowestUnder = 0  # 最低点
    nTradeCnt = 0  # 总交易次数
    nWinTrade = 0  # 盈利交易次数
    nLossTrade = 0  # 亏损交易次数
    nEntryPrice = 0  # buy入价
    nEntryRow = 0  # buy入所在行
    nDaysOfTrade = 0  # 策略执行工作日天数
    nCapital = 0  # 账户期末价值
    nProfit = 0  # 盈利金额
    nLoss = 0  # 亏损金额
    nStopLoss = 0  # StopLoss点
    nInitialCapital = 0
    nEquity = 0  # 策略开始运行时账户总价值
    nMaxEquity = 0  # 策略开始运行时账户最大总价值

    # 未持仓,归零
    date = 0
    TradeType = 0
    nTradePrice = 0
    nExpectedStopLoss_r = 0
    nTradeProfit_r = 0
    nTradeProfitAnaly_r = 0
    nTradeProfitDay_r = 0
    nTradeProfitVolatility_r = 0
    nSharpe_r = 0
    nWin_r = 0
    nGainLoss_r = 0
    nMaxDrawdown_r = 0

    #创建空，接收回测结果
    df_trade_to_csv = pd.DataFrame(
        columns=('date', 'TradeType', 'nTradePrice', 'nExpectedStopLoss_r', 'nTradeProfit_r',
                 'nTradeProfitAnaly_r', 'nTradeProfitVolatility_r', 'nSharpe_r', 'nWin_r',
                 'nGainLoss_r', 'nMaxDrawdown_r', 'nCapital'))
    # #创建空，接收每日策略收益，计算波动率
    ls_nTradeProfitDay_r = []
    # 按行遍历csv（每日操作）
    for nRow in range(1, len(local_data)):
        nClose = local_data.loc[nRow, 'price_f']
        nPreClose = local_data.loc[nRow - 1, 'price_f']
        nFinzdaPrice = local_data.loc[nRow, fair_value]
        nPreFinzdaPrice = local_data.loc[nRow - 1, fair_value]

        # holding
        if nOI == 1:
            nTradeProfitDay_r = (nClose - nPreClose) / nPreClose
            ls_nTradeProfitDay_r.append(nTradeProfitDay_r)

            # nLowestUnder 找最低点，自上而下穿过均线，记录收盘价最低点
            if nClose < nFinzdaPrice:
                if nLowestUnder == 0 or nPreClose > nPreFinzdaPrice or nLowestUnder > nClose:
                    nLowestUnder = nClose

            # nhighestUp 找最高点，自下而上穿过均线，记录收盘价最高点
            if nClose > nFinzdaPrice:
                if nhighestUp == 0 or nPreClose < nPreFinzdaPrice or nhighestUp < nClose:
                    nhighestUp = nClose

            # Exit,Stopwin
            if nEntryPrice < nClose:
                if nPreClose >= nPreFinzdaPrice and nClose < nFinzdaPrice:
                    TradeType = 'Stopwin'
                    nExitPrice = nClose
                    date = local_data.loc[nRow, 'date']
                    nTradePrice = nExitPrice
                    nWinTrade = nWinTrade + 1

                    nTradeProfit_r = (nExitPrice - nEntryPrice) / nEntryPrice
                    nTradeProfit = nTradeProfit_r * nCapital  #金额
                    nProfit = nProfit + nTradeProfit

            # Exit,StopLoss
            if nClose < nStopLoss:
                TradeType = 'StopLoss'
                nExitPrice = nClose
                date = local_data.loc[nRow, 'date']
                nTradePrice = nExitPrice
                nLossTrade = nLossTrade + 1

                nTradeLoss_r = (nExitPrice - nEntryPrice) / nEntryPrice
                nTradeLoss = nTradeLoss_r * nCapital    #金额
                nTradeProfit_r = nTradeLoss_r
                nLoss = nLoss + nTradeLoss

            if TradeType == 'Stopwin' or TradeType == 'StopLoss':
                date = local_data.loc[nRow, 'date']
                nDaysOfTrade = nDaysOfTrade + (nRow - nEntryRow)
                nOI = 0
                nExpectedStopLoss_r = 0
                nTradeProfitAnaly_r = ((1 + nTradeProfit_r) ** (250 / nDaysOfTrade) - 1)

                # 求 nTradeProfitDay_r 策略每日收益率 的样本标准差，n-1，无偏的，ddof = 1, df['close'][0:5].std(ddof=1)
                nTradeProfitVolatility_r = 250 ** (1 / 2) * np.std(ls_nTradeProfitDay_r,
                    ddof=1)  # 计算样本方差
                nSharpe_r = (nTradeProfitAnaly_r - 0.04) / nTradeProfitVolatility_r
                nWin_r = nWinTrade / nTradeCnt

                if nLoss==0:
                    nGainLoss_r = 0
                if nLoss!=0:
                    nGainLoss_r = - nProfit / nLoss
                nMaxDrawdown_r = -(nLowestUnder - nhighestUp) / nLowestUnder
                nCapital = nCapital * (nTradePrice / nEntryPrice)    #账户金额
                print('卖', nCapital)
                ls_nTradeProfitDay_r = []

                df_trade_to_csv = df_trade_to_csv.append(
                    pd.DataFrame(
                        {'date': [date], 'TradeType': [TradeType], 'nTradePrice': [nTradePrice],
                         'nExpectedStopLoss_r': [nExpectedStopLoss_r], 'nTradeProfit_r': [nTradeProfit_r],
                         'nTradeProfitAnaly_r': [nTradeProfitAnaly_r],
                         'nTradeProfitVolatility_r': [nTradeProfitVolatility_r],
                         'nSharpe_r': [nSharpe_r], 'nWin_r': [nWin_r],
                         'nGainLoss_r': [nGainLoss_r],
                         'nMaxDrawdown_r': [nMaxDrawdown_r], 'nCapital': [nCapital]}),
                    ignore_index=True)

        # not_hold
        if nOI == 0:
            nTradeProfitDay_r = 0
            # Entry
            if nPreClose > 0 and nPreClose <= nPreFinzdaPrice and nClose > nFinzdaPrice:
                nEntryRow = nRow
                nStopLoss = nLowestUnder
                nEntryPrice = nClose
                date = local_data.loc[nEntryRow, 'date']
                TradeType = 'buy'
                nTradePrice = nEntryPrice
                nExpectedStopLoss_r = (nStopLoss - nEntryPrice) / nEntryPrice

                nTradeCnt = nTradeCnt + 1
                nOI = 1
                if nTradeCnt == 1:
                    nCapital = local_data.loc[nEntryRow, 'price']
                    nInitialCapital = nCapital
                print('buy', nCapital)
                # buy入后，将其他值初始化
                nTradeProfit_r = 0
                nTradeProfitAnaly_r = 0
                nTradeProfitVolatility_r = 0
                nSharpe_r = 0
                nWin_r = 0
                nGainLoss_r = 0
                nMaxDrawdown_r = 0

                df_trade_to_csv = df_trade_to_csv.append(
                    pd.DataFrame(
                        {'date': [date], 'TradeType': [TradeType], 'nTradePrice': [nTradePrice],
                         'nExpectedStopLoss_r': [nExpectedStopLoss_r], 'nTradeProfit_r': [nTradeProfit_r],
                         'nTradeProfitAnaly_r': [nTradeProfitAnaly_r],
                         'nTradeProfitVolatility_r': [nTradeProfitVolatility_r],
                         'nSharpe_r': [nSharpe_r], 'nWin_r': [nWin_r],
                         'nGainLoss_r': [nGainLoss_r],
                         'nMaxDrawdown_r': [nMaxDrawdown_r], 'nCapital': [nCapital]}),
                    ignore_index=True)

    df_trade_to_csv.to_csv('{}_StrategyBacktestingStatus.csv'.format(code), index=False, encoding='utf-8')


if __name__ == '__main__':
    FairPrice_Strategy("600519", 'Peter_Lynch_Fair_Value_f')
