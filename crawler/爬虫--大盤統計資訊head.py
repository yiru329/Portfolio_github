# _*_ coding:utf-8   _*_
import csv
import os
# import re
import OpenSSL
from urllib.request import urlopen
# from bs4 import BeautifulSoup
import json
import datetime
import pandas as pd
import urllib.request
import random
import time
# import randomsleep
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

__author__ = 'yara'

'''
二：从js/dat等文件中把数据读出来(table 下没内容)  https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=&type=&_=1563437229310
'''

# 在处理文本的时候，首先要做的是读取一下文本文件，一般的方法如下：
# 路径不存在是需要新建
sDir = r'U:\yiru\涨跌证券数合计--tw'
sTitleName = '大盤統計資訊-成交筆數1.csv'
if not os.path.exists(sDir):
    os.mkdir(sDir)
sCSV_Path = os.path.join(sDir, sTitleName);

# with open(sCSV_Path, 'r', newline='', encoding='utf-8') as csvFile:  # 创建可写文件
#     reader = csvFile.readlines()
#     last_line = reader[-1].split(',')[0]
#     # start = datetime.strptime(last_line, '%Y%m%d')

if __name__ == '__main__':
    iCntTable = 0;
    delta = datetime.timedelta(days=1)

    # start = datetime.datetime.strptime(last_line, '%Y%m%d')
    start = datetime.datetime(2004, 2, 10)  #2012, 1, 1
    end = datetime.datetime.now()
    # print(start)
    # print(type(start))

    with open(sCSV_Path, 'w', newline='', encoding='utf-8') as csvFile:  # 创建可写文件
         writer = csv.writer(csvFile)
         writer.writerow(['1', '2', '3', '4', '5','6', '7', '8', '9', '10', '11', '12', '13', '14',
                          '15', '16'])  # 写入一行

    fmt = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date1:%Y%m%d}&type=&_=0'
    # f = open('U:\yiru\涨跌证券数合计--tw/url-list.txt', 'r')

    while start < end:
        date1 = start
        date2 = start + delta
        html_1 = fmt.format(date1=date2)
        print(html_1)
        print(type(html_1))
        # html_2 = html_1 + '\n'
        # f.writelines(url2)
    # f.close()
        time.sleep(random.random() * 7)

        with urllib.request.urlopen(html_1) as url:
            my_html = json.loads(url.read().decode('utf-8').replace("'", '"'))
            print(my_html)

            if my_html["stat"] == "OK" and "data7" in my_html.keys():
                print(my_html["data7"])
                print(my_html["date"])
#                 print(type(my_html["data7"]))
#                 b = my_html["data7"][0][1];
#                 print(b)
#                 # print(len((my_html)['data8']))

                with open(sCSV_Path, 'a', newline='', encoding='utf-8') as csvFile:  # 创建可写文件
                    writer = csv.writer(csvFile)
                    date_b = []
                    date_b.append(my_html["date"])
                    date_b.append(my_html["data7"][0][3])
                    date_b.append(my_html["data7"][1][3])
                    date_b.append(my_html["data7"][2][3])
                    date_b.append(my_html["data7"][3][3])
                    date_b.append(my_html["data7"][4][3])
                    date_b.append(my_html["data7"][5][3])
                    date_b.append(my_html["data7"][6][3])
                    date_b.append(my_html["data7"][7][3])
                    date_b.append(my_html["data7"][8][3])
                    date_b.append(my_html["data7"][9][3])
                    date_b.append(my_html["data7"][10][3])
                    date_b.append(my_html["data7"][11][3])
                    date_b.append(my_html["data7"][12][3])
                    date_b.append(my_html["data7"][13][3])
                    date_b.append(my_html["data7"][14][3])
                    writer.writerow(date_b)  # 写入一行
            else:
                pass
        start = date2



