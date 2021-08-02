# _*_ coding:utf-8   _*_
import pandas as pd
import numpy as np
import random
from sklearn.preprocessing import minmax_scale
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from sklearn.cluster import k_means
from sklearn.metrics import silhouette_score
from matplotlib import pyplot as plt
from numpy import mean
from functools import reduce

__author__ = 'yara'

'''
数据来源：真实信贷业务数据经过模糊处理构成
模型算法：银行信贷客户进行聚类--划分簇;逻辑回归--生成评分

字段内容：
CUST_ID	
Loan_Type	
Loan_Term	
Start_Date	
End_Date	
Loan_Amt	
Undisbursed_Amt	
Business_Type_Code	
Business_Type_Name	
Credit_Ind	
Interest_Rate	
Repay_Way	
Pay_Way	
Interest_Payment	
Marriage	
Marriage_Interpret	
Edu_Level	
Monthky_Income	
Job	Industry	
Rural	
External_Ind	
Credit_Level	
Gender	
Age
'''

if __name__ == '__main__':
    # 原始数据源
    loan_table = pd.read_csv(r'F:\ren\upup\data analysis\data_analysis-bank_loan/loan details.csv',
                             header=0, encoding='gb2312')
    ## 简单 数据探索
    print('数据规模：行*列', loan_table.shape)
    print('数据字段信息', loan_table.info())
    print('数据分布', loan_table.describe())

    ## 数据清洗
    # 去除因信息重复提交的ID 。keep : {‘first’, ‘last’, False}, default ‘first’ 删除重复项并保留第一次出现的项
    loan_table.drop_duplicates(['CUST_ID', 'Loan_Type', 'Loan_Term', 'Start_Date', 'End_Date',
                                'Loan_Amt', 'Undisbursed_Amt', 'Business_Type_Code'], keep="first",
                               inplace=True)
    # 空值补缺
    loan_table['Interest_Payment'].fillna(value=9, inplace=True)
    loan_table['Credit_Level'].fillna(value=9, inplace=True)

    ## 数据变换
    # 将字符串变为列表数据类型
    all_vars = list(loan_table.columns)
    all_vars.remove('CUST_ID')
    for var in all_vars:
        loan_table[var] = loan_table[var].apply(lambda x: [x])

    id_loans_group = loan_table.groupby('CUST_ID').sum()
    print('客户分组', id_loans_group)
    # 衍生变量（总值、平均值）
    # No. of loan types
    var1 = id_loans_group.apply(lambda x: len(set(x.Loan_Type)), axis=1)
    var1 = var1.to_frame(name='No_Loan_Types')
    # count of loans
    var2 = id_loans_group.apply(lambda x: len(x.Loan_Type), axis=1)
    var2 = var2.to_frame(name='No_Loan')

    # 需要归一化
    # max of loan terms 逾期类型
    var3a = id_loans_group.apply(lambda x: max(x.Loan_Term), axis=1)
    var3a = var3a.to_frame(name='Max_Loan_Terms')
    # min of loan terms
    var3b = id_loans_group.apply(lambda x: min(x.Loan_Term), axis=1)
    var3b = var3b.to_frame(name='Min_Loan_Terms')
    # mean of loan terms
    var3c = id_loans_group.apply(lambda x: mean(x.Loan_Term), axis=1)
    var3c = var3c.to_frame(name='Mean_Loan_Terms')
    # total loan amount
    # var4 = id_loans.groupby('CUST_ID').Loan_Amt.sum()
    var4a = id_loans_group.apply(lambda x: sum(x.Loan_Amt), axis=1)
    var4a = var4a.to_frame(name='Total_Loan_Amt')
    # mean loan amount
    var4b = id_loans_group.apply(lambda x: mean(x.Loan_Amt), axis=1)
    var4b = var4b.to_frame(name='Mean_Loan_Amt')
    # total Undisbursed_Amt
    # var5 = id_loans.groupby('CUST_ID').Undisbursed_Amt.sum()
    var5a = id_loans_group.apply(lambda x: sum(x.Undisbursed_Amt), axis=1)
    var5a = var5a.to_frame(name='Total_Undisbursed_Amt')
    # mean Undisbursed_Amt
    var5b = id_loans_group.apply(lambda x: mean(x.Undisbursed_Amt), axis=1)
    var5b = var5b.to_frame(name='Mean_Undisbursed_Amt')
    # min and max of interest rate of the single customer
    var7a = id_loans_group.apply(lambda x: min(x.Interest_Rate), axis=1)
    var7a = var7a.to_frame(name='Min_Interest_Rate')
    var7b = id_loans_group.apply(lambda x: max(x.Interest_Rate), axis=1)
    var7b = var7b.to_frame(name='Max_Interest_Rate')

    # ratio of total Undisbursed_Amt and total loan amount  额度使用率
    var6a = pd.concat([var4a, var5a], axis=1)
    var6a['Total_Undisbursed_to_Loan'] = var6a.apply(lambda x: x.Total_Undisbursed_Amt / x.Total_Loan_Amt, axis=1)
    del var6a['Total_Undisbursed_Amt']
    del var6a['Total_Loan_Amt']
    # ratio of mean Undisbursed_Amt and mean loan amount
    var6b = pd.concat([var4b, var5b], axis=1)
    var6b['Mean_Undisbursed_to_Loan'] = var6b.apply(lambda x: x.Mean_Undisbursed_Amt / x.Mean_Loan_Amt, axis=1)
    del var6b['Mean_Undisbursed_Amt']
    del var6b['Mean_Loan_Amt']

    # 数值型：归一化
    transfer = minmax_scale(feature_range=(0, 1))  # 实例化一个转换器
    array_minmax_vars = transfer.fit_transform(id_loans_group[
                                             ['Max_Loan_Terms', 'Min_Loan_Terms', 'Mean_Loan_Terms', 'Total_Loan_Amt', 'Mean_Loan_Amt',
                                              'Total_Undisbursed_Amt', 'Mean_Undisbursed_Amt',
                                              'Min_Interest_Rate', 'Max_Interest_Rate']])
    df_minmax_vars = pd.DataFrame(array_minmax_vars.values, columns=[['Max_Loan_Terms', 'Min_Loan_Terms', 'Mean_Loan_Terms', 'Total_Loan_Amt', 'Mean_Loan_Amt',
                                              'Total_Undisbursed_Amt', 'Mean_Undisbursed_Amt',
                                              'Min_Interest_Rate', 'Max_Interest_Rate']])
    derived_features = pd.concat(
        [var1, var2, var6a, var6b, df_minmax_vars], axis=1)
    # 类别型 独热编码 onehot encode
    var_onehot_list = ['Business_Type_Code', 'Repay_Way', 'Interest_Payment', 'Rural', 'External_Ind', 'Credit_Level',
                       'Gender']
    for var_onehot in var_onehot_list:
        var_onehot_df = id_loans_group[var_onehot]
        var_onehot_df = var_onehot_df.to_frame(name=var_onehot)
        new_columns = []  # 一个类别特征生成多个列
        # OneHotEncoder不能直接处理字符串值。 若名义特征是字符串，那么需要先把它们映射成整数。
        label = LabelEncoder()  #实例化 贴标签
        oneHot = OneHotEncoder()  #实例化
        la_var = label.fit_transform(var_onehot_df).reshape(-1, 1)
        for cla in label.classes_:
            new_columns.append(col + '_' + cla)
        array_var_onehot = oneHot.fit_transform(la_var).toarray()
        df_var_onehot = pd.DataFrame(array_var_onehot, columns=new_columns)
        derived_features = pd.concat([derived_features, df_var_onehot], axis=1)
    # 删除列
    # we need to remove the below columns since  their coded dualities exist in the file   编码对偶存在于文件中
    var_dropped_list = ['Business_Type_Name', 'Marriage_Interpret']
    for var_dropped in var_dropped_list:
        del id_loans[var_dropped]
    # Moreover,some columns have high percentage of missing values, so we also need to remove them.  列的缺失值百分比很高
    var_missing_list = ['Edu_Level', 'Monthky_Income', 'Job', 'Industry']
    for var_missing in var_missing_list:
        del id_loans[var_missing]

    ## 模型算法：聚类
    # 样本，利用elbow法判断出最优簇数（图像结果看出来，不明显）
    M = 1000
    # array_dataset = np.matrix(derived_features)[:M, ]
    df_dataset = derived_features[:M, ]
    cost = []
    score = []
    for k in range(2, 7):
        model = k_means(df_dataset, n_clusters=k)
        cost.append(model.inertia_)
    plt.plot(range(2, 7), cost[:5])
    plt.xlabel("number of clusters")
    plt.ylabel("cost of clustering")
    plt.title("Elbow method")
    plt.show()

    # 评估聚类结果
    model = k_means(df_dataset, n_clusters=3)
    score = silhouette_score(df_dataset, model[1])
    print('聚类个数为3时，轮廓函数：', score)












