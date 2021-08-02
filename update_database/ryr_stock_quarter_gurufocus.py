# _*_ coding:utf-8   _*_
import sys
from EmQuantAPI import *
import urllib.request, json
from urllib.request import Request, urlopen
# import numpy
import pandas as pd
# import requests
from pandas.io.json import json_normalize
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
import os
import logging
import csv
import codecs
import time
import decimal
import datetime
import logging_module  # 自定义的日志模块
import mysql_support
import shutil
import numpy as np

__author__ = 'yara'

'''每日更新，定时启动
逻辑与choice的大致相似，但是由于gurufocus的数据延迟，更新股票数据的时间设置为：实际披露日期的5天后'''


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
            # 更新日期频率,现在更新已经实际披露公告5天了的数据
            five_day = datetime.timedelta(days=5)
            update_time_date = datetime.datetime.strptime(update_time, '%Y-%m-%d')
            update_time = (update_time_date - five_day).strftime('%Y-%m-%d')

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
            data_write_csv('r_{}_gurufocus.csv'.format(reportdate1), local_path, cb_update_list,
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
def cb_update(local_path):
    file_names = ['r_0331_gurufocus.csv', 'r_0630_gurufocus.csv', 'r_0930_gurufocus.csv', 'r_1231_gurufocus.csv']
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
            # 将待更新的code列表逐个更新，判断运行哪个公司类型函数：def
            need_update_list_df_remain = run_which_type(need_update_df)
            # 存入剔除已更新后剩余的dataframe：def
            need_update_list_df_remain.to_csv(all_file_path, index=False)


# 运行公司抓取数据函数
def run_which_type(need_update_df):
    for index, row in need_update_df.iterrows():
        print('索引：', index)
        print('dataframe的一行：', index)
        one_code = row['code']
        report_date = row['report_date']
        get_data_return = get_data(one_code)
        if get_data_return is False:
            logging.warning('{},更新 失败'.format(one_code))
            # return False
        else:
            # 将更新好的数据存入数据库：def
            clear_data_stock_df_to_sql(report_date)

            # 读取一行，删除一行，列表删除用pop；dataframe删除用drop
            need_update_df.drop(index=[index], inplace=True)
            logging.debug('{},更新 完毕'.format(one_code))
    print('剔除已更新后,剩余的dataframe：', need_update_df)
    logging.debug('剔除已更新后,剩余的dataframe 完毕')
    return need_update_df


# 处理gurufocus数据，将josn格式转为csv
def save_to_csvfile(data, stockcode, strDict):
    series = pd.Series(data['financials'][strDict])
    initial = 0
    if (len(series.index) > 2):
        for index in series.index:
            if index == 'Fiscal Year' or index == 'Preliminary':
                continue
            dftemp = pd.DataFrame(series[index])
            dftemp = dftemp.T
            dftemp.columns = series['Fiscal Year']
            if initial == 0:
                dfall = dftemp
                initial = 1
            else:
                dfall = pd.concat([dfall, dftemp], axis=0)
        all_csv_path = csv_path + '/' + str(stockcode) + '_' + strDict + '.csv'
        dfall.to_csv(all_csv_path, encoding='utf-8-sig')
        t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('%s %s 下载完成!' % (t, all_csv_path))


# 从网页上获取gurufocus数据
def get_data(code, attempt=0):
    if attempt < 3:
        for one_code in code:
            gur_code = one_code[-2:] + 'SE' + ':' + one_code[0:6]
            try:
                req = Request(
                    'https://api.gurufocus.com/public/user/e4c6ed5d0358f25b4e6a53aa98498075:ec150d27e8d034b1c66914495be2e933/stock/{}/financials '.format(
                        gur_code),
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'})

                content = urlopen(req).read()
                data = json.loads(content.decode('utf-8'))

                # 下载年度数据：def
                # save_to_csvfile(data, one_code, 'annuals')

                # 下载季度数据：def
                save_to_csvfile(data, one_code, 'quarterly')
                return True
            except Exception as e:
                error_class = e.__class__.__name__  # 获取错误类型

                detail = e.args[0]  # 获取详细内容
                t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print('%s %s %s' % (t, error_class, detail))

                print('%s %s 下載失敗!!!' % (t, one_code))
    else:
        logging.critical('3次尝试失败')
        return False


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


# 两个list 求差集（在B中但不在A中），找出缺少的标题列表
def diff(listA):
    listB = ['index', 'Altman Z-Score', 'Piotroski F-Score', 'Beneish M-Score ', 'Tangible Book per Share',
             'Peter Lynch Fair Value', 'Net Current Asset Value', 'Earnings Power Value (EPV)',
             'Property, Plant and Equipment', 'Common Stock', 'Preferred Stock', 'Additional Paid-In Capital',
             'Treasury Stock', 'Quick Ratio', 'Days Payable', 'Days Sales Outstanding', 'Days Inventory ',
             'Cash Conversion Cycle', 'Asset Turnover', 'Operating Margin %', 'Interest Coverage']
    retE = [i for i in listB if i not in listA]
    print("差集retE is: ", retE)
    return retE


# 历史数据：pandas清洗csv，存入数据库
def clear_data_stock_df_to_sql(report_date):
    # 计算Python的代码块或程序的运行时间
    start = datetime.datetime.now()
    # 生成Excel全路径
    if os.path.isdir(csv_path):
        # # 只取code列表里的文件
        # csv_allpath_list = []
        # for one_code in code:
        #     one_code_fileName = one_code + '_quarterly.csv'
        #     csv_allpath_list.append(one_code_fileName)

        # 文件夹里全是想要的文件
        csv_allpath_list = os.listdir(csv_path)

        csv_allpath_list.sort()  # 文件按照数字排序
        for fileName in csv_allpath_list:
            # print(fileName)             #文件夹里Excel单个名称
            csv_allpath = os.path.join(csv_path, fileName)
            print(csv_allpath)

            # pandas读Excel并清洗
            data = pd.read_csv(
                csv_allpath)  # 指定标题的行索引  , header=1 跳过第一行, skiprows=1   取特定的sheet, sheet_name='uncertain'
            # 去掉第一列数字序号,因为是索引，不能直接去掉
            # data = data.iloc[:, 1:]  # 选取DataFrame的所有行，并截取第二列至最末列。
            data = data.T
            # 将列标签设置为等于第二行(索引位置1)中的值:
            data.columns = data.iloc[0]
            data = data.iloc[1:]  # 选取DataFrame的第二行至最末行，保留所有列，并将选取的数据表保存在一个新的变量中。    # 进行转置
            data = data.reset_index()  # 将索引装换为列

            # # 如果索引具有唯一标签，则可以使用以下命令删除第二行:   不好用，删不掉，所以直接如上进行切片了
            # data.drop(data.index[0])
            # # 如果索引不是唯一的，则可以使用:
            # df.iloc[pd.RangeIndex(len(df)).drop(1)]

            # 两个list 求差集（在B中但不在A中），找出缺少的标题列表:def
            listA = data.columns.values.tolist()
            print('listA', listA)
            diff_list = diff(listA)
            for diff_one in diff_list:
                data['{}'.format(diff_one)] = np.nan
            # print('第er个', data)
            # # #在原有列表上追加
            # listA.extend(diff_list)
            # print('拼接列表', listA)

            # 将需要的指标提前重命名
            data.rename(
                columns={'index': 'date', 'Altman Z-Score': 'z_score', 'Piotroski F-Score': 'f_score',
                         'Beneish M-Score': 'm_score', 'Tangible Book per Share': 'tangible_book_value',
                         'Peter Lynch Fair Value': 'peter_lynch_value', 'Net Current Asset Value': 'nca_value',
                         'Earnings Power Value (EPV)': 'epv_value', 'Property, Plant and Equipment': 'p_p_e',
                         'Common Stock': 'common_s', 'Preferred Stock': 'preferred_s',
                         'Additional Paid-In Capital': 'a_p_i_c', 'Treasury Stock': 'treasury_s',
                         'Quick Ratio': 'quick_ratio', 'Days Payable': 'days_p', 'Days Sales Outstanding': 'days_s',
                         'Days Inventory ': 'days_inv', 'Cash Conversion Cycle': 'days_cash_conversion',
                         'Asset Turnover': 'asset_turnover', 'Operating Margin %': 'operating_margin',
                         'Interest Coverage': 'Int_coverage'}, inplace=True)

            # 将日期列改为标准4个季度日期
            data['date'] = data['date'].apply(
                lambda x: x[:4] + '-12-31' if 9 < int(x[5:7]) <= 12 else (
                    x[:4] + '-09-30' if 6 < int(x[5:7]) <= 9 else (
                        x[:4] + '-06-30' if 3 < int(x[5:7]) <= 6 else (
                            x[:4] + '-03-31' if 1 <= int(x[5:7]) <= 3 else 0))))

            data['code'] = fileName[:9]
            data = data[['code', 'date', 'z_score', 'f_score', 'm_score', 'tangible_book_value', 'peter_lynch_value',
                         'nca_value', 'epv_value', 'p_p_e', 'common_s', 'preferred_s', 'a_p_i_c', 'treasury_s',
                         'quick_ratio', 'days_p', 'days_s', 'days_inv', 'days_cash_conversion', 'asset_turnover',
                         'operating_margin', 'Int_coverage']]  # 选取多列，多列名字要放在list里
            data = data.replace('-', np.nan)
            data = data.replace('N/A', np.nan)
            data = data.replace('No Debt', np.nan)
            # data.fillna(value=np.nan, inplace=True)
            #转换成小数
            data = data.apply(pd.to_numeric, errors='ignore')
            # 将date时间格式化'2019-01-01',转换为没有时分秒的日期
            data['date'] = pd.to_datetime(data['date'], format="%Y-%m-%d")
            data["date"] = data["date"].dt.date
            # 截取需要更新的行
            data = data[data['date'] == '{}'.format(report_date)]
            # data.fillna(value=max_decimal, inplace=True)

            data['p_p_e'] = data.apply(lambda x: x['p_p_e'] * 1000000 if x['p_p_e'] != 0 else pd.NaT,
                                                 axis=1)
            data['common_s'] = data.apply(lambda x: x['common_s'] * 1000000 if x['common_s'] != 0 else pd.NaT,
                                                 axis=1)
            data['preferred_s'] = data.apply(lambda x: x['preferred_s'] * 1000000 if x['preferred_s'] != 0 else pd.NaT,
                                                 axis=1)
            data['a_p_i_c'] = data.apply(lambda x: x['a_p_i_c'] * 1000000 if x['a_p_i_c'] != 0 else pd.NaT,
                                                 axis=1)
            data['treasury_s'] = data.apply(lambda x: x['treasury_s'] * 1000000 if x['treasury_s'] != 0 else pd.NaT,
                                                 axis=1)
            data['operating_margin'] = data.apply(
                lambda x: x['operating_margin'] / 100,
                axis=1)

            print(data)
            data.to_sql(name=tableName, con=engine, if_exists='append', index=False)  # fail replace
            print('存入数据库 完毕')

            # 将数据入库
            data.to_sql(name=tableName, con=engine, if_exists='append', index=False)  # fail replace

            # 将入库情况写入本地csv
            DLS_df1 = pd.DataFrame(
                {'date': '{}'.format(update_time), 'size': '{}'.format(data.size), 'state': 'update succeeded'},
                index=[0])
            DLS_df1_path = local_path + '/CN_monitor'
            # def
            df_to_csv(DLS_df1_path, 'DLS_{}.csv'.format(tableName), DLS_df1, ['date', 'size', 'state'])
            logging.debug('股票日行情,存入数据库，size {} 完毕'.format(data.size))

            # #拼存入大表，主要未了避免code外键约束
            # # data.to_csv('uncertain_csv/{}.csv'.format(name), encoding='utf-8', index=False)
            # df_to_csv(csv_path, 'quarter_all_add_column.csv', data, ['code', 'date', 'z_score ', 'f_score ', 'm_score ', 'tangible_book_value', 'peter_lynch_value', 'nca_value', 'epv_value'], attempt=0)

    # # 入库完毕就删掉这个文件夹
    # shutil.rmtree(csv_path)
    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://ryr:black@192.168.10.121:3306/CN', echo=False)
    # 调用自定义日志模块：def
    logging_module.set_logging('ryr_stock_quarter_gurufocus')
    # 调用登录函数（激活后使用，不需要用户名密码）
    try:
        login = c.start('ForceLogin = 1')
        logging.debug('choice_login,开始获取财务报表数据')
    except Exception as ex:

        logging.error('接口登录失败，{}'.format(ex))

    # 本地路径
    local_path = r'C:/Users/YiRu/PycharmProjects/currency_remind/database'
    # 存入数据库的路径
    csv_path = r'C:\Users\YiRu\PycharmProjects\currency_remind\database\A_gurufocus_quarterly'
    tableName = 'stock_quarter_gurufocus'
    # # 将所有空值填为decimal的最大值
    # max_decimal = decimal.MAX_EMAX
    # df1 = pd.read_excel(r'D:\工作\gurufocus拉数据\全部A股.xlsx')
    # stock_list = df1['证券代码'].dropna().to_list()

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
        cb_update(local_path)  # 更新最新财报
        if update_time > today_time:
            break
