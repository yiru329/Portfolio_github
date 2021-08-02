# _*_ coding:utf-8   _*_
from sqlalchemy import *
import pandas as pd
import logging

__author__ = 'yara'

def sample():
    # 此为数据库事务结合 pandas 示例
    engine = create_engine('mysql+pymysql://ysq:black@192.168.10.121:3306/china', echo=False)

    df = pd.DataFrame({'name': [10000.000], 'hsrtr':
                       ['13579.SH']})
    df1 = pd.DataFrame({'name': [1234.000], 'hsrtr':
                        ['17886.SH']})
    # 开始事物
    with engine.begin() as conn:
        df.to_sql(name='test', con=conn, if_exists='append',
                  index=False, index_label='id')
        # 故意出现错误的代码，测试事物回滚
        df1.to_sql(name='test', con=conn, if_exists='append')


engine = create_engine('mysql+pymysql://ryr:black@192.168.10.121:3306/china', echo=False)
# 获取 table 更新时间
def get_update_time(engine):
    db = engine.connect()
    update_datetime = "SELECT a.date FROM stock_2020 a WHERE NOT EXISTS(SELECT 1 FROM (select * from stock_2020 order by date desc LIMIT 1) b WHERE b.date>a.date) LIMIT 1;"
    # update_datetime = "select TABLE_NAME,UPDATE_TIME from information_schema.TABLES where TABLE_SCHEMA='china' and information_schema.TABLES.TABLE_NAME = '{}';".format(table_name)
    result = db.execute(update_datetime)
    # print(result)
    # print(result.fetchall())
    # print(type(result.fetchall()))
    # print(len(result.fetchall()))
    # #判断列表是否为空
    # if result.fetchall():
    #     print('Do something with my list')
    # else:
    #     print('The list is empty')
    # print('先打印完后就无法引用了！！！', result.fetchall()[0][0])

    update_time = result.fetchall()[0][0]
    # update_time = result.fetchall()[0][0].strftime('%Y-%m-%d')
    # update_time = result.fetchall()[0][1].strftime('%Y-%m-%d')
    print('222', update_time)
    db.close()
    return update_time


# 删除上一次更新的数据
def delete_latest(engine, table_name, date):
    conn = engine.connect()
    db = engine.connect().begin()
    sql_command = "delete from {} where date='{}' ".format(table_name, date)
    try:
        conn.execute(sql_command)
        db.commit()
        logging.info('{}，表{}历史数据删除成功！'.format(date, table_name))
    except Exception as ex:
        # 出错即回滚，不保存任何修改
        db.rollback()
        logging.error('数据库错误 {}'.format(ex))
    finally:
        db.close()


# 基本资料表，删除上一次更新的数据
def ipo_delete_latest(engine, table_name, date):
    conn = engine.connect()
    db = engine.connect().begin()
    sql_command = "delete from {} where ipo_date='{}' ".format(table_name, date)
    try:
        conn.execute(sql_command)
        db.commit()
        logging.info('{}，表{}历史数据删除成功！'.format(date, table_name))
    except Exception as ex:
        # 出错即回滚，不保存任何修改
        db.rollback()
        logging.error('数据库错误 {}'.format(ex))
    finally:
        db.close()


#修改groupby配置
def set_groupby(engine):
    conn = engine.connect()
    db = engine.connect().begin()
    sql_command = "SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY,',''));"
    try:
        conn.execute(sql_command)
        db.commit()
        logging.info('分组配置成功！')
    except Exception as ex:
        # 出错即回滚，不保存任何修改
        db.rollback()
        logging.error('数据库错误 {}'.format(ex))
    finally:
        db.close()


