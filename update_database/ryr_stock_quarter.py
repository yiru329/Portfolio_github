# -*- coding:utf-8 -*-
from EmQuantAPI import *
from sqlalchemy import create_engine
import pandas as pd
import datetime
import numpy as np
import decimal
import time
import random
import os
import csv
import codecs
import logging
import mysql_support
import shutil
import logging_module  # 自定义的日志模块

__author__ = 'yara'

'''每日更新，定时启动'''
'''根据财报公告日期更新，按照财务报表类型和首发日分类（只要当年有新股上市,年报随时会有更新)
table1:   证券代码，首发上市日期
         code，ipo_date
table2:   证券代码，财务报表类型，定期报告实际披露日期，更正补充公告披露日期,报告期
         code，cb_type, actual_disclosure_date, supplementary_disclosure_date,report_date
all_date=c.css("000001.SZ","FINANSTATETYPE,LASTREPORTDATE,STMTACTDATE,CORRECTANNCDATE",
         "ispandas=1, ReportDate=2018-12-31,Type=0,FormType=0")
'''

'''每日更新，获取所有公告日期,分成4个报告期  table:   证券代码，财务报表类型，定期报告实际披露日期，更正补充公告披露日期
   财报数据，每日公告进行筛选，得到今日更新的list，写入csv（追加），将csv读成列表取前200行，进行判断加载数据，读一行删除一行，将新list重新写入csv（重写）'''


# 生成股票代码列表列表:excel
def excel_to_list(file_all_name: str, index_name: str):
    need_list = []
    data = pd.read_excel(file_all_name)
    for i in range(len(data)):
        need_list.append(data[index_name][i])
    print(need_list)
    return need_list


# code为全部股票代码，yesterday_format为前一天的日期，report_date为报告期'-03-31','-06-30','-09-30','-12-31'
def api_update_df(code, update_time, attempt=0):
    if attempt < 3:
        try:
            # 获得当前年份
            year_now = update_time.split('-')[0]
            # 年报取去年
            last_year = eval(update_time.split('-')[0]) - 1
            report_date = ['{}-12-31'.format(last_year), '{}-03-31'.format(year_now), '{}-06-30'.format(year_now),
                           '{}-09-30'.format(year_now)]
            # 获得季报更新时间
            for reportdate in report_date:
                api_update_df1 = c.css(code, "FINANSTATETYPE,STMTPLANDATE,CORRECTANNCDATE",
                                       "ispandas=1,Type=0,FormType=0,ReportDate={}".format(reportdate))
                api_update_df2 = api_update_df1.reset_index('CODES')  # 将索引转换为列
                api_update_df2.pop('DATES')  # 删除下载数据当日日期
                api_update_df2.columns = ['code', 'cb_type', 'actual_disclosure_date', 'supplementary_disclosure_date']
                # all_date.sort_values("actual_disclosure_date", inplace=True)    #按照某列排序
                # 转换日期格式xxxx-xx-xx
                # season_one_data['actual_disclosure_date'] = pd.to_datetime(season_one_data['actual_disclosure_date'])
                # 增加最新报告期列
                api_update_df2['report_date'] = reportdate
                # 将接口数据存入列表：def
                dealed_cb_update_list(update_time, local_path, api_update_df2, reportdate)
                time.sleep(3)

        except AttributeError as at:
            print('已知的错误（接口不稳定/）：{}'.format(at))
            return False

        except Exception as ex:
            logging.error('季报披露公告，接口调用错误，{}'.format(ex))
            attempt += 1
            time.sleep(3)
            api_update_df(code, update_time, attempt)
            print(ex)
            logging.debug(ex)
    else:
        logging.critical('{},最新公告 失败'.format(update_time))
        # for one_code in code:
        api_update_df1 = c.css('000001.SZ', "FINANSTATETYPE,STMTPLANDATE,CORRECTANNCDATE",
                               "Type=0,FormType=0,ReportDate={}".format('2019-12-31'))
        logging.info('最新公告数据,接口报错代码:ErrorCode:{},ErrorMsg:{}'.format(api_update_df1.ErrorCode, api_update_df1.ErrorMsg))
        # col = ['cb_type', 'actual_disclosure_date', 'supplementary_disclosure_date']
        # api_update_df3 = to_df(api_update_df1, code, 3, col, reportdate)
        # 将dataframe写入csv，防止标题行重复;csv_path创建路径,csv_name 文件csv名称：def
        # df_to_csv(local_path, 'r_api_update.csv', api_update_df3,
        #           ['code', 'date', 'cb_type', 'actual_disclosure_date', 'supplementary_disclosure_date',
        #            'report_date'])
        return False


# 筛选今日待更新的列表(实际披露，补充披露）
def dealed_cb_update_list(update_time, local_path, api_update_df2, reportdate, attempt=0):
    if attempt < 3:
        try:
            cb_update_list = []
            for a in api_update_df2.iterrows():
                if a[1]['actual_disclosure_date'] == str(update_time):
                    cb_update_list.append(a[1])
                elif a[1]['supplementary_disclosure_date'] == str(update_time):
                    cb_update_list.append(a[1])
                    # cb_update_list.append('\n')  #不能加换行，否则会有空行
            print('筛选,{},{},最新公告'.format(update_time, reportdate), cb_update_list)
            logging.debug('筛选,{},{},最新公告 完毕'.format(update_time, reportdate))
            # 存入所有待更新的csv:def
            reportdate1 = reportdate.replace('-', '')[-4:]
            data_write_csv('r_{}.csv'.format(reportdate1), local_path, cb_update_list,
                           ['code', 'cb_type', 'actual_disclosure_date', 'supplementary_disclosure_date',
                            'report_date'])
            time.sleep(3)

        except Exception as ex:
            logging.error('筛选,{},{},最新公告 错误'.format(update_time, reportdate))
            attempt += 1
            time.sleep(3)
            dealed_cb_update_list(update_time, local_path, api_update_df2, reportdate, attempt)
            print(ex)
            logging.debug(ex)
    else:
        logging.critical('筛选,{},{},最新公告 失败'.format(update_time, reportdate))
        return False


# （写）将list按行写入csv：file_name为写入CSV文件的路径，a_w为写入方式， update_list为要写入的数据列表, index_list为要增加的标题
def data_write_csv(file_name, file_path, update_list, index_list, attempt=0):
    if attempt < 3:
        try:
            # 判断本地是否有该csv文件，没有就新建一个带标题的文件(采用相对路径)
            all_file_path = os.path.join(file_path, file_name)
            if not os.path.exists(all_file_path):
                file_csv = codecs.open(all_file_path, 'a', 'utf-8')  # 追加'a',读写'w+'
                writer = csv.writer(file_csv, delimiter=',', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(index_list)
                print("带标题的csv文件创建成功")
                logging.debug('带标题的csv文件,{},创建成功 完毕'.format(file_name))
            else:
                file_csv = codecs.open(file_name, 'a', 'utf-8')  # 追加'a',读写'w+'
                writer = csv.writer(file_csv, delimiter=',', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for data in update_list:
                writer.writerow(data)
            print("保存文件成功")
            logging.debug('保存文件,{},成功 完毕'.format(file_name))
        except Exception as ex:
            logging.error('筛选的待更新列表写入本地 错误，{}'.format(ex))
            attempt += 1
            time.sleep(3)
            data_write_csv(file_name, update_list, index_list, attempt)
            print(ex)
            logging.debug(ex)
    else:
        logging.critical('3次尝试失败')


# 进行每日的更新财报任务
def cb_update(tableName, local_path):
    file_names = ['r_0331.csv', 'r_0630.csv', 'r_0930.csv', 'r_1231.csv']
    for file_name in file_names:
        print(file_name)
        all_file_path = os.path.join(local_path, file_name)
        # 取出所有待更新的dataframe
        need_update_df = pd.read_csv(all_file_path)
        print('所有待更新的dataframe：', need_update_df)
        # 判断是否为空dataframe
        if need_update_df.empty:
            logging.debug('所有待更新的dataframe为空,今日无待更新财报数据')
        else:
            # 判断运行哪个公司类型函数：def
            need_update_list_df_remain = run_which_type(need_update_df)
            # 存入剔除已更新后剩余的dataframe：def
            need_update_list_df_remain.to_csv(all_file_path, index=False)
            stock_df_to_sql(tableName, csv_path)  # 将本地数据存入数据库


# 运行公司抓取数据函数
def run_which_type(need_update_df):
    # word[:2] 一维列表/dataframe取前两行;遍历二维列表,取前200行;range(n)从0到n;iterrows,将DataFrame的每一行迭代为(index, Series)对，
    # 可以通过row[name]对元素进行访问;这里遍历dataframe后为（3699，（series））元祖；for index, row in df1.iterrows()
    if len(need_update_df) < 200:
        need_update_df_today = need_update_df
    else:
        need_update_df_today = need_update_df[:200]
    for index, row in need_update_df_today.iterrows():
        print('索引：', index)
        print('dataframe的一行：', index)
        one_code = row['code']
        report_date = row['report_date']
        stock_quarter_general_return = stock_quarter_general(csv_path, update_time, one_code,
                                                             report_date)
        if stock_quarter_general_return is False:
            logging.warning('{},更新 失败'.format(one_code))
            # return False
        else:
            # 读取一行，删除一行，列表删除用pop；dataframe删除用drop
            need_update_df.drop(index=[index], inplace=True)
            logging.debug('{},更新 完毕'.format(one_code))
    print('剔除已更新后,剩余的dataframe：', need_update_df)
    logging.debug('剔除已更新后,剩余的dataframe 完毕')
    return need_update_df


# （写）将dataframe写入csv，防止标题行重复;csv_path创建路径,csv_name 文件csv名称
def df_to_csv(csv_path, csv_name, update_df, index_list, attempt=0):
    if attempt < 3:
        try:
            update_list = []
            for a in update_df.iterrows():
                update_list.append(a[1])
            if not os.path.exists(csv_path):
                # 生成文件夹，将接口获取的数据存入本地（当死机时有地方存放未更新的数据）
                os.mkdir(csv_path)
            csv_all_path = os.path.join(csv_path, csv_name)
            # data.to_csv(csv_all_path, mode='a', index=False)  # 追加写入，去掉索引号,去掉标题,检查用 , mode='a', header=False,index=False
            # 判断本地是否有该csv文件，没有就新建一个带标题的文件(采用相对路径)
            if not os.path.exists(csv_all_path):
                file_csv = codecs.open(csv_all_path, 'a', 'utf-8')  # 追加'a',读写'w+'
                writer = csv.writer(file_csv, delimiter=',', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(index_list)
                print("带标题的csv文件创建成功")
                logging.debug('带标题的csv文件,{},创建成功 完毕'.format(csv_name))
            else:
                file_csv = codecs.open(csv_all_path, 'a', 'utf-8')  # 追加'a',读写'w+'
                writer = csv.writer(file_csv, delimiter=',', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for data in update_list:
                writer.writerow(data)
            print("保存文件成功")
            logging.debug('保存文件,{},成功 完毕'.format(csv_name))
        except Exception as ex:
            logging.error('财报数据写入本地 错误，{}'.format(ex))
            attempt += 1
            time.sleep(3)
            df_to_csv(csv_path, csv_name, update_df, index_list, attempt)
            print(ex)
            logging.debug(ex)
    else:
        logging.critical('3次尝试失败')


# 1.财报数据及财报分析指标-一般公司 css 截面数据,ShowBlank=0
def stock_quarter_general(csv_path, update_time, one_code, report_date, attempt=0):
    if attempt < 3:
        try:
            # 沪深股票 每股净资产BPS 非经常性损益 资本支出TTM（报告期） 当期计提折旧与摊销 企业自由现金流量FCFF 息税前利润EBIT(正推法) 每股股利(税前) 流动比率 资产总计 负债合计 股东权益合计 货币资金 流动资产合计 流动负债合计 非流动负债合计 单季度.研发费用 财务报表类型 总股本 单季度.利息支出
            data1 = c.css(one_code,
                          "BPS,EXTRAORDINARY,CAPEXR,DA,FCFF,EBITDRIVE,DIVCASHPSBFTAX,CURRENTTATIO,BALANCESTATEMENT_74,TBALANCESTATEMENT_128,BALANCESTATEMENT,BALANCESTATEMENT_9,BALANCESTATEMENT_25,BALANCESTATEMENT_93,BALANCESTATEMENT_103,INCOMESTATEMENTQ_89,FINANSTATETYPE,TOTALSHARE,INCOMESTATEMENTQ_20,TBALANCESTATEMENT_132",
                          "type=1,AssignFeature=1,YesNo=1,EndDate={},ReportDate={},Curtype=1,ItemsCode=64".format(
                              update_time, report_date))
            # choice格式转换成dataframe,带有指标名称：def
            data1_2 = to_df(data1, one_code, report_date)
            time.sleep(random.random() * 2)

            # 沪深股票 每股收益EPS(TTM) 净资产收益率ROE(TTM) 净利润(TTM) 息税前利润(TTM) 营业总收入(TTM) 总资产报酬率ROA(TTM) 销售毛利率(TTM) 营业收入(TTM) 销售费用(TTM) 管理费用(TTM) 财务费用(TTM) 所得税(TTM) 营业利润(TTM) 利润总额(TTM)
            data2 = c.css(one_code,
                          "EPSTTM,ROETTM,NITTM,EBITTTM,GRTTM,ROATTM,GPMARGINTTM,ORTTM,OPERATEEXPENSETTM,ADMINEXPENSETTM,FINAEXPENSETTM,TAXTTM,OPTTM,EBTTTM",
                          "TradeDate={},TtmType=1,CurType=1".format(report_date))
            # choice格式转换成dataframe,带有指标名称：def
            data2_2 = to_df(data2, one_code, report_date)
            time.sleep(random.random() * 2)

            # 沪深股票 经营活动现金净流量(TTM) 投资活动现金净流量(TTM) 市盈率(PE,TTM)
            data3 = c.css(one_code,
                          "CFOTTM,CFITTM,PETTM",
                          "TradeDate={},CurType=1,TtmType=1".format(report_date))
            # choice格式转换成dataframe,带有指标名称：def
            data3_2 = to_df(data3, one_code, report_date)
            # print('一般公司财报分析指标', data1_2)
            logging.debug('{},数据获取 完毕'.format(one_code))
            time.sleep(random.random() * 2)

            # 因股票代码和日期完全相同，故选择内连接；若股票代码不相同，选择外连接做匹配
            data_1 = pd.merge(data1_2, data2_2, how='inner', on=['code', 'date'])
            data = pd.merge(data_1, data3_2, how='inner', on=['code', 'date'])
            # print('一般公司', data_2)
            # data = data_2.fillna(value=max_decimal)  # 将空值转换成零
            print('第一个', data)
            # 两个list 求差集（在B中但不在A中），找出缺少的标题列表:def
            listA = data.columns.values.tolist()
            df_to_csv(csv_path, 'r_stock_quarter.csv', data, listA)
            logging.debug('一般公司,{}，存入本地 完毕'.format(one_code))
            return True

        # except AttributeError as at:
        #     print('已知的错误（接口不稳定/）：{}'.format(at))
        #     return False

        except Exception as ex:
            logging.error('一般公司，接口调用错误，{}'.format(ex))
            attempt += 1
            time.sleep(15)
            stock_quarter_general(csv_path, update_time, one_code, report_date, attempt)
            print(ex)
            logging.debug(ex)
    else:
        logging.critical('3次尝试失败')
        return False


# choice格式转换成dataframe：def
def to_df(em_data, code, report_date):
    if em_data.ErrorCode != 0:
        logging.error('接口错误：', em_data.ErrorCode, em_data.ErrorMsg)
        return False
    try:
        df_data = pd.DataFrame(em_data.Data[code], index=em_data.Indicators)
        df_data = df_data.T
        df_data_list = df_data.columns.values.tolist()
        df_data['code'] = code
        df_data['date'] = report_date
        sequence = ['code', 'date']
        num = len(df_data)
        for i in range(num):
            df_data.rename(columns={i: df_data_list[i]}, inplace=True)
        for i in df_data_list:
            sequence.append(i)
        df_data = df_data[sequence]
        # print(df_data)
        logging.info('数据获取成功')
        return df_data
    except Exception as ex:
        logging.error('报错：', ex)


# 将本地数据写入数据库
def stock_df_to_sql(tableName, csv_path, attempt=0):
    if attempt < 3:
        try:
            # 开始事务管理（防止已经从接口取到的数据再次调用接口）；直接从本地文件夹读取，存入数据库之后，再删除该文件
            with engine.begin() as conn:
                if os.path.isdir(csv_path):
                    for fileName in os.listdir(csv_path):
                        # print(fileName)             #文件夹里Excel单个名称
                        csv_allpath = os.path.join(csv_path, fileName)
                        print(csv_allpath)  # 文件夹里Excel单个名称

                        # 读本地数据dataframe，导入数据库，分表存入
                        stock_df = pd.read_csv(csv_allpath)
                        stock_df.rename(
                            columns={'EPSTTM': 'eps_ttm', 'ROETTM': 'roe_ttm', 'NITTM': 'ni_ttm', 'BPS': 'bps',
                                     'EXTRAORDINARY': 'no_recur_items', 'CAPEXR': 'cap_expend_ttm', 'DA': 'depre_amort',
                                     'FCFF': 'fcff', 'EBITTTM': 'ebit_ttm  ', 'BALANCESTATEMENT_74': 'total_assets',
                                     'GRTTM': 'gr_ttm', 'CFOTTM': 'cfo_ttm', 'CFITTM': 'cfi_ttm',
                                     'DIVCASHPSBFTAX': 'dividend_before_tax', 'ROATTM': 'roa_ttm',
                                     'CURRENTTATIO': 'current_ratio', 'GPMARGINTTM': 'gross_margin_ttm',
                                     'TBALANCESTATEMENT_128': 'liability', 'BALANCESTATEMENT': 'equity',
                                     'BALANCESTATEMENT_9': 'cash_equivalents', 'BALANCESTATEMENT_25': 'current_assets',
                                     'BALANCESTATEMENT_93': 'current_liabilities',
                                     'BALANCESTATEMENT_103': 'long_liabilities', 'ORTTM': 'op_revenue_ttm',
                                     'INCOMESTATEMENTQ_89': 'rd_cost_sgl', 'OPERATEEXPENSETTM': 'sale_cost_ttm',
                                     'ADMINEXPENSETTM': 'manage_cost_ttm', 'FINAEXPENSETTM': 'fin_cost_ttm',
                                     'TAXTTM': 'income_tax_ttm', 'OPTTM': 'operating_profit_ttm',
                                     'EBTTTM': 'profit_total_ttm', 'FINANSTATETYPE': 'cb_type', 'PETTM': 'pe_ttm',
                                     'TOTALSHARE': 'total_share', 'INCOMESTATEMENTQ_20': 'int_exp_sgl', 'TBALANCESTATEMENT_132': 'r_e'}, inplace=True)

                        stock_df['roe_ttm'] = stock_df.apply(
                            lambda x: x['roe_ttm'] / 100, axis=1)
                        stock_df['roa_ttm'] = stock_df.apply(lambda x: x['roa_ttm'] / 100,
                                                       axis=1)
                        stock_df['gross_margin_ttm'] = stock_df.apply(lambda x: x['gross_margin_ttm'] / 100,
                                                        axis=1)

                        stock_df.to_sql(name=tableName, con=conn, if_exists='append', index=False)  # fail replace
                        logging.debug('财报数据,{},存入数据库，size {} 完毕'.format(fileName, stock_df.size))

                        # 将入库情况写入本地csv
                        DLS_df1 = pd.DataFrame(
                            {'date': '{}'.format(update_time), 'size': '{}'.format(stock_df.size),
                             'state': 'update succeeded'}, index=[0])
                        DLS_df1_path = local_path + '/CN_monitor'
                        # def
                        df_to_csv(DLS_df1_path, 'DLS_{}.csv'.format(tableName), DLS_df1, ['date', 'size', 'state'])

            logging.debug('财报数据,存入数据库，size {} 完毕'.format(stock_df.size))
            # 删除文件夹
            shutil.rmtree(csv_path)
            time.sleep(10)

        except Exception as ex:
            logging.error('股票财报数据,存入数据库 错误，{}'.format(ex))
            attempt += 1
            time.sleep(10)
            stock_df_to_sql(tableName, csv_path, attempt)
            print(ex)
            logging.debug(ex)
    else:
        logging.critical('3次尝试失败')
        # 删除文件夹
        shutil.rmtree(csv_path)
        return False


if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://ryr:black@192.168.10.121:3306/CN', echo=False)
    # 调用自定义日志模块：def
    logging_module.set_logging('ryr_stock_quarter')
    # 调用登录函数（激活后使用，不需要用户名密码）
    try:
        login = c.start('ForceLogin = 1')
        logging.debug('choice_login,开始获取财务报表数据')
    except Exception as ex:
        logging.error('接口登录失败，{}'.format(ex))

    # 本地路径
    local_path = r'C:/Users/YiRu/PycharmProjects/currency_remind/database'
    # 存入数据库的路径
    csv_path = r'C:\Users\YiRu\PycharmProjects\currency_remind\database\stock_quarter'
    tableName = 'stock_quarter'
    # # 将所有空值填为decimal的最大值，增加列
    # max_decimal = decimal.MAX_EMAX
    # 获取该表在数据库的最新更新日期
    # update_time = '2020-09-18'
    update_time = mysql_support.get_update_time(engine)
    update_time_date = datetime.datetime.strptime(update_time, '%Y-%m-%d')
    # 更新日期频率
    one_day = datetime.timedelta(days=1)
    # # 表更新日期的前一天
    # update_time_before_1 = (update_time_date - one_day).strftime('%Y-%m-%d')
    # update_time_before_1_date = datetime.datetime.strptime(update_time_before_1, '%Y-%m-%d')
    # 当日日期
    today_time = datetime.datetime.now().strftime('%Y-%m-%d')
    today_time_date = datetime.datetime.strptime(today_time, '%Y-%m-%d')
    # # 当日日期的前一天
    # today_time_before = (today_time_date - one_day).strftime('%Y-%m-%d')
    # # 两日期差距
    # interval = (today_time_date - update_time_date).days
    # print(interval)

    while update_time <= today_time:
        update_time_date = datetime.datetime.strptime(update_time, '%Y-%m-%d')
        update_time = (update_time_date + one_day).strftime('%Y-%m-%d')
        # #获取固定的所有股票代码
        # code = excel_to_list(r'C:\Users\YiRu\PycharmProjects\currency_remind\database\stock_type_ipo_20200803.xlsx', '证券代码')
        # 接口获取股票日行情数据

        # 从数据库stock表中获取所有股票代码，以防变更，与choice最新的比对，取交集
        df_code = pd.read_sql("SELECT code FROM stock;", engine)
        code = df_code['code'].to_list()
        # code2 = c.sector("401001", update_time).Codes
        # code = list(set(code1).intersection(set(code2)))

        print(update_time)
        # 每日更新，获取所有公告，筛选出今日待更新的dataframe，加上之前未更新完毕的，每日更新不超过200条
        api_update_df(code, update_time)  # 获取更新公告
        cb_update(tableName, local_path)  # 更新最新财报
        if update_time > today_time:
            break

    # # 判断需要更新的日期（表内时间=表更新时间的前一天 < 表更新时间=今天日期,今天更新昨天的数据：财报， [不确定日期，日行情9:00am]）
    # if update_time < today_time:
    #     for i in range(interval):
    #         delta_day = datetime.timedelta(days=i + 1)
    #         update_time = (update_time_before_1_date + delta_day).strftime('%Y-%m-%d')
    #         code = c.sector("001004", update_time).Codes
    #         print(update_time)
    #         # 每日更新，获取所有公告，筛选出今日待更新的dataframe，加上之前未更新完毕的，每日更新不超过200条
    #         api_update_df(code, update_time)  # 获取更新公告
    #         cb_update(tableName, local_path)  # 更新最新财报
    # # elif update_time == today_time:
    # #     update_time = today_time
    # #     code = c.sector("001004", update_time).Codes
    # #     print(update_time)
    # #     # 每日更新，获取所有公告，筛选出今日待更新的dataframe，加上之前未更新完毕的，每日更新不超过200条
    # #     api_update_df(code, update_time)  # 获取更新公告
    # #     cb_update(tableName, local_path)  # 更新最新财报

    # report_date = '2018-12-31'
    # update_time = '2019-01-23'
    # # one_code = '000002.SZ'  # 一般
    # one_code = '601628.SH'  # 保险
    # # one_code = '000001.SZ' #银行
    # # one_code = '601688.SH' #证券
    # # stock_quarter_general(max_decimal, csv_path, update_time, one_code, report_date)
    # stock_quarter_insur(max_decimal, csv_path, update_time, one_code, report_date)
    # # stock_quarter_bank(max_decimal, csv_path, update_time, one_code, report_date)
    # # stock_quarter_secur(max_decimal, csv_path, update_time, one_code, report_date)
