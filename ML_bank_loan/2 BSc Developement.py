import pandas as pd
import numpy as np
import pickle
import random
import datetime
from sklearn.model_selection import train_test_split
from statsmodels.stats.outliers_influence import variance_inflation_factor
import statsmodels.api as sm
from sklearn import ensemble
import matplotlib.pyplot as plt

'''银行信贷客户行为评分卡--特征工程、训练测试'''

# 逾期类型特征
def DelqFeatures(event, window, type):
    current = 12
    start = 12 - window + 1
    delq1 = [event[a] for a in ['Delq1_' + str(t) for t in range(current, start - 1, -1)]]
    delq2 = [event[a] for a in ['Delq2_' + str(t) for t in range(current, start - 1, -1)]]
    delq3 = [event[a] for a in ['Delq3_' + str(t) for t in range(current, start - 1, -1)]]
    if type == 'max delq':
        if max(delq3) == 1:
            return 3
        elif max(delq2) == 1:
            return 2
        elif max(delq1) == 1:
            return 1
        else:
            return 0
    if type in ['M0 times', 'M1 times', 'M2 times']:
        if type.find('M0') > -1:
            return sum(delq1)
        elif type.find('M1') > -1:
            return sum(delq2)
        else:
            return sum(delq3)


# 额度使用率
def UrateFeatures(event, window, type):
    current = 12
    start = 12 - window + 1
    monthlySpend = [event[a] for a in ['Spend_' + str(t) for t in range(current, start - 1, -1)]]
    limit = event['Loan_Amount']
    monthlyUrate = [x / limit for x in monthlySpend]
    if type == 'mean utilization rate':
        return np.mean(monthlyUrate)
    if type == 'max utilization rate':
        return max(monthlyUrate)
    if type == 'increase utilization rate':
        currentUrate = monthlyUrate[0:-1]
        previousUrate = monthlyUrate[1:]
        compareUrate = [int(x[0] > x[1]) for x in zip(currentUrate, previousUrate)]
        return sum(compareUrate)


def PaymentFeatures(event, window, type):
    current = 12
    start = 12 - window + 1
    currentPayment = [event[a] for a in ['Payment_' + str(t) for t in range(current, start - 1, -1)]]
    previousOS = [event[a] for a in ['OS_' + str(t) for t in range(current - 1, start - 2, -1)]]
    monthlyPayRatio = []
    for Pay_OS in zip(currentPayment, previousOS):
        if Pay_OS[1] > 0:
            payRatio = Pay_OS[0] * 1.0 / Pay_OS[1]
            monthlyPayRatio.append(payRatio)
        else:
            monthlyPayRatio.append(1)
    if type == 'min payment ratio':
        return min(monthlyPayRatio)
    if type == 'max payment ratio':
        return max(monthlyPayRatio)
    if type == 'mean payment ratio':
        total_payment = sum(currentPayment)
        total_OS = sum(previousOS)
        if total_OS > 0:
            return total_payment / total_OS
        else:
            return 1


if __name__==__main__:
    #################################
    #   1, 读取数据，衍生初始变量   #
    #################################

    creditData = pd.read_csv(r'F:\ren\upup\data analysis\data_analysis-bank_loan/behavioural data.csv',
                             header=0)

    # 自带模块，随机划分训练集和测试集
    trainData, testData = train_test_split(creditData, train_size=0.7)

    trainData.to_csv(r'F:\ren\upup\data analysis\data_analysis-bank_loan/trainData.csv', index=False)
    testData.to_csv(r'F:\ren\upup\data analysis\data_analysis-bank_loan/testData.csv', index=False)

    trainData = pd.read_csv(r'F:\ren\upup\data analysis\data_analysis-bank_loan/trainData.csv', header=0)
    testData = pd.read_csv(r'F:\ren\upup\data analysis\data_analysis-bank_loan/testData.csv', header=0)

    allFeatures = []
    '''
    逾期类型的特征在行为评分卡（预测违约行为）中，一般是非常显著的变量。
    通过设定时间窗口，可以衍生以下类型的逾期变量：
    '''
    # #逾期类型特征 def DelqFeatures 考虑过去1个月，3个月，6个月，12个月
    for t in [1, 3, 6, 12]:
        # 1，过去t时间窗口内的最大逾期状态
        allFeatures.append('maxDelqL' + str(t) + "M")
        trainData['maxDelqL' + str(t) + "M"] = trainData.apply(lambda x: DelqFeatures(x, t, 'max delq'), axis=1)

        # 2，过去t时间窗口内的，M0,M1,M2的次数
        allFeatures.append('M0FreqL' + str(t) + "M")
        trainData['M0FreqL' + str(t) + "M"] = trainData.apply(lambda x: DelqFeatures(x, t, 'M0 times'), axis=1)

        allFeatures.append('M1FreqL' + str(t) + "M")
        trainData['M1FreqL' + str(t) + "M"] = trainData.apply(lambda x: DelqFeatures(x, t, 'M1 times'), axis=1)

        allFeatures.append('M2FreqL' + str(t) + "M")
        trainData['M2FreqL' + str(t) + "M"] = trainData.apply(lambda x: DelqFeatures(x, t, 'M2 times'), axis=1)

    '''
    额度使用率类型特征在行为评分卡模型中，通常是与违约高度相关的
    '''
    # 额度使用率 def UrateFeatures 考虑过去1个月，3个月，6个月，12个月
    for t in [1, 3, 6, 12]:
        # 1，过去t时间窗口内的最大月额度使用率
        allFeatures.append('maxUrateL' + str(t) + "M")
        trainData['maxUrateL' + str(t) + "M"] = trainData.apply(lambda x: UrateFeatures(x, t, 'max utilization rate'),
                                                                axis=1)

        # 2，过去t时间窗口内的平均月额度使用率
        allFeatures.append('avgUrateL' + str(t) + "M")
        trainData['avgUrateL' + str(t) + "M"] = trainData.apply(lambda x: UrateFeatures(x, t, 'mean utilization rate'),
                                                                axis=1)

        # 3，过去t时间窗口内，月额度使用率增加的月份。该变量要求t>1
        if t > 1:
            allFeatures.append('increaseUrateL' + str(t) + "M")
            trainData['increaseUrateL' + str(t) + "M"] = trainData.apply(
                lambda x: UrateFeatures(x, t, 'increase utilization rate'),
                axis=1)

    '''
    def PaymentFeatures 还款类型特征也是行为评分卡模型中常用的特征
    '''
    # 考虑过去1个月，3个月，6个月，12个月
    for t in [1, 3, 6, 12]:
        # 1，过去t时间窗口内的最大月还款率
        allFeatures.append('maxPayL' + str(t) + "M")
        trainData['maxPayL' + str(t) + "M"] = trainData.apply(lambda x: PaymentFeatures(x, t, 'max payment ratio'),
                                                              axis=1)

        # 2，过去t时间窗口内的最小月还款率
        allFeatures.append('minPayL' + str(t) + "M")
        trainData['minPayL' + str(t) + "M"] = trainData.apply(lambda x: PaymentFeatures(x, t, 'min payment ratio'),
                                                              axis=1)

        # 3，过去t时间窗口内的平均月还款率
        allFeatures.append('avgPayL' + str(t) + "M")
        trainData['avgPayL' + str(t) + "M"] = trainData.apply(lambda x: PaymentFeatures(x, t, 'mean payment ratio'),
                                                              axis=1)

    '''
    类别型变量：过去t时间内最大的逾期状态
    需要检查与bad的相关度
    '''
    trainData.groupby(['maxDelqL1M'])['label'].mean()
    trainData.groupby(['maxDelqL3M'])['label'].mean()
    trainData.groupby(['maxDelqL6M'])['label'].mean()
    trainData.groupby(['maxDelqL12M'])['label'].mean()

    for x in allFeatures:
        for y in allFeatures:
            if x != y:
                print(x, '   ', y, '   ', np.corrcoef(trainData[x], trainData[y])[0, 1])

    ############################
    #   2, 分箱，计算WOE并编码   #
    ############################
    '''
    对类别型变量的分箱和WOE计算
    可以通过计算取值个数的方式判断是否是类别型变量
    '''
    categoricalFeatures = []
    numericalFeatures = []
    WOE_IV_dict = {}
    for var in allFeatures:
        if len(set(trainData[var])) > 5:
            numericalFeatures.append(var)
        else:
            categoricalFeatures.append(var)

    not_monotone = []
    for var in categoricalFeatures:
        # 检查bad rate在箱中的单调性
        if not BadRateMonotone(trainData, var, 'label'):
            not_monotone.append(var)

    # 'M2FreqL3M', 'M1FreqL3M'，'maxDelqL12M' 是不单调的，需要合并其中某些类别
    trainData.groupby(['M2FreqL3M'])['label'].mean()  # 检查单调性
    '''
    M2FreqL3M
    0    0.121793
    1    0.949932
    2    0.943396
    3    0.000000
    '''

    trainData.groupby(['M2FreqL3M'])['label'].count()  # 其中，M2FreqL3M＝3总共只有1个样本，因此要进行合并
    '''
    M2FreqL3M
    0    32473
    1      739
    2       53
    3        1
    '''

    # 将 M2FreqL3M>=1的合并为一组，计算WOE和IV
    trainData['M2FreqL3M_Bin'] = trainData['M2FreqL3M'].apply(lambda x: int(x >= 1))
    trainData.groupby(['M2FreqL3M_Bin'])['label'].mean()
    WOE_IV_dict['M2FreqL3M_Bin'] = CalcWOE(trainData, 'M2FreqL3M_Bin', 'label')
    # 'WOE': {0: 0.16936574889989442, 1: -4.9438414919038607}, 'IV': 0.79574732036209472


    trainData.groupby(['M1FreqL3M'])['label'].mean()  # 检查单调性
    '''
    M1FreqL3M
    0    0.049511
    1    0.409583
    2    0.930825
    3    0.927083
    '''
    trainData.groupby(['M1FreqL3M'])['label'].count()
    '''
    0    22379
    1     4800
    2      824
    3       96
    '''
    # 除了M1FreqL3M＝3外， 其他组别的bad rate单调。
    # 此外，M1FreqL3M＝0 占比很大，因此将M1FreqL3M>=1的分为一组
    trainData['M1FreqL3M_Bin'] = trainData['M1FreqL3M'].apply(lambda x: int(x >= 1))
    trainData.groupby(['M1FreqL3M_Bin'])['label'].mean()
    WOE_IV_dict['M1FreqL3M_Bin'] = CalcWOE(trainData, 'M1FreqL3M_Bin', 'label')
    # 'WOE': {0: 1.1383566599817516, 1: -1.7898564039068015}, 'IV': 1.7515413713105232


    '''
    对其他单调的类别型变量，检查是否有一箱的占比低于5%。 如果有，将该变量进行合并
    '''
    small_bin_var = []
    large_bin_var = []
    for var in categoricalFeatures:
        if var not in not_monotone:
            pcnt = BinPcnt(trainData, var)
            if pcnt['min'] < 0.05:
                small_bin_var.append({var: pcnt['each pcnt']})
            else:
                large_bin_var.append(var)

    '''
    {'maxDelqL1M': {0: 0.60379372931421049, 1: 0.31880138083205806, 2: 0.069183956724438597, 3: 0.0082209331292928574}}
    {'M2FreqL1M': {0: 0.99177906687070716, 1: 0.0082209331292928574}}
    {'maxDelqL3M': {0: 0.22637816292394747, 1: 0.57005587387451506, 2: 0.18068258656891703, 3: 0.022883376632620377}}
    {'maxDelqL6M': {0: 0.057226235809103528, 1: 0.58489625965336844, 2: 0.31285810882949572, 3: 0.045019395708032317}}
    {'M2FreqL6M': {0: 0.95498060429196774, 1: 0.04003701199330937, 2: 0.0045909107085661408, 3: 0.00032029609594647497, 4: 7.1176910210327775e-05}}
    {'M2FreqL12M': {0: 0.92334246770347694, 1: 0.066514822591551295, 2: 0.0092174098722374465, 3: 0.00081853446741876937, 4: 0.00010676536531549166}}
    '''
    # 对于M2FreqL1M，由于M2FreqL1M＝0占了全部的99%，需要将该变量舍弃
    # 对于M2FreqL6M，由于M2FreqL6M＝0占了95%，需要将该变量舍弃
    allFeatures.remove('M2FreqL1M')
    allFeatures.remove('M2FreqL6M')
    # 对于small_bin_var中的其他变量，将最小的箱和相邻的箱进行合并并计算WOE
    trainData['maxDelqL1M_Bin'] = trainData['maxDelqL1M'].apply(lambda x: MergeByCondition(x, ['==0', '==1', '>=2']))
    trainData['maxDelqL3M_Bin'] = trainData['maxDelqL3M'].apply(lambda x: MergeByCondition(x, ['==0', '==1', '>=2']))
    trainData['maxDelqL6M_Bin'] = trainData['maxDelqL6M'].apply(lambda x: MergeByCondition(x, ['==0', '==1', '>=2']))
    trainData['M2FreqL12M_Bin'] = trainData['M2FreqL12M'].apply(lambda x: MergeByCondition(x, ['==0', '>=1']))
    for var in ['maxDelqL1M_Bin', 'maxDelqL3M_Bin', 'maxDelqL6M_Bin', 'M2FreqL12M_Bin']:
        WOE_IV_dict[var] = CalcWOE(trainData, var, 'label')

    '''
    对于不需要合并、原始箱的bad rate单调的特征，直接计算WOE和IV
    '''
    for var in large_bin_var:
        WOE_IV_dict[var] = CalcWOE(trainData, var, 'label')

    '''
    对于数值型变量，需要先分箱，再计算WOE、IV
    分箱的结果需要满足：
    1，箱数不超过5
    2，bad rate单调
    3，每箱占比不低于5%
    '''
    bin_dict = []
    for var in numericalFeatures:
        binNum = 5
        newBin = var + '_Bin'
        bin = ChiMerge_MaxInterval(trainData, var, 'label', max_interval=binNum, minBinPcnt=0.05)
        trainData[newBin] = trainData[var].apply(lambda x: AssignBin(x, bin))
        # 如果不满足单调性，就降低分箱个数
        while not BadRateMonotone(trainData, newBin, 'label'):
            binNum -= 1
            bin = ChiMerge_MaxInterval(trainData, var, 'label', max_interval=binNum, minBinPcnt=0.05)
            trainData[newBin] = trainData[var].apply(lambda x: AssignBin(x, bin))
        WOE_IV_dict[newBin] = CalcWOE(trainData, newBin, 'label')
        bin_dict.append({var: bin})

    ##############################
    #   3, 单变量分析和多变量分析   #
    ##############################
    #  选取IV高于0.02的变量
    high_IV = [(k, v['IV']) for k, v in WOE_IV_dict.items() if v['IV'] >= 0.02]
    high_IV_sorted = sorted(high_IV, key=lambda k: k[1], reverse=True)
    for var in high_IV:
        newVar = var + "_WOE"
        trainData[newVar] = trainData[var].map(lambda x: WOE_IV_dict[var]['WOE'][x])

    '''
    多变量分析：比较两两线性相关性。如果相关系数的绝对值高于阈值，剔除IV较低的一个
    '''
    deleted_index = []
    cnt_vars = len(high_IV_sorted)
    for i in range(cnt_vars):
        if i in deleted_index:
            continue
        x1 = high_IV_sorted[i][0] + "_WOE"
        for j in range(cnt_vars):
            if i == j or j in deleted_index:
                continue
            y1 = high_IV_sorted[j][0] + "_WOE"
            roh = np.corrcoef(trainData[x1], trainData[y1])[0, 1]
            if abs(roh) > 0.7:
                x1_IV = high_IV_sorted[i][1]
                y1_IV = high_IV_sorted[j][1]
                if x1_IV > y1_IV:
                    deleted_index.append(j)
                else:
                    deleted_index.append(i)

    single_analysis_vars = [high_IV_sorted[i][0] + "_WOE" for i in range(cnt_vars) if i not in deleted_index]

    '''
    多变量分析：VIF
    '''
    X = np.matrix(trainData[single_analysis_vars])
    VIF_list = [variance_inflation_factor(X, i) for i in range(X.shape[1])]
    # 最大的VIF是3.57，小于10，因此这一步认为没有多重共线性
    multi_analysis = single_analysis_vars

    ################################
    #   4, 建立逻辑回归模型预测违约   #
    ################################
    X = trainData[multi_analysis]
    X['intercept'] = [1] * X.shape[0]
    y = trainData['label']
    logit = sm.Logit(y, X)
    logit_result = logit.fit()
    pvalues = logit_result.pvalues
    params = logit_result.params
    fit_result = pd.concat([params, pvalues], axis=1)
    fit_result.columns = ['coef', 'p-value']
    '''
                                   coef        p-value
    maxDelqL3M_Bin_WOE        -0.796816  8.730886e-205
    M1FreqL6M_Bin_WOE         -0.048587   2.068982e-01
    M2FreqL3M_Bin_WOE         -0.690943   1.243145e-55
    M1FreqL12M_Bin_WOE        -0.136644   9.594563e-04
    avgPayL3M_Bin_WOE         -0.452555   1.376075e-38
    maxDelqL1M_Bin_WOE        -0.104840   8.769247e-04
    M0FreqL6M_Bin_WOE         -0.060990   1.479443e-01
    avgPayL6M_Bin_WOE         -0.109368   2.750289e-02
    minPayL3M_Bin_WOE         -0.009197   8.754071e-01
    M0FreqL12M_Bin_WOE         0.077267   1.897971e-01
    M2FreqL12M_Bin_WOE         0.058466   3.817414e-01
    avgPayL12M_Bin_WOE        -0.042265   5.390312e-01
    minPayL1M_Bin_WOE          0.216368   4.120280e-03
    increaseUrateL6M_Bin_WOE  -1.412843  8.408589e-100
    maxPayL6M_Bin_WOE          0.008202   9.252952e-01
    avgUrateL1M_Bin_WOE       -0.620488   8.579870e-10
    minPayL6M_Bin_WOE          0.162593   1.328954e-01
    avgUrateL3M_Bin_WOE       -0.299872   1.517677e-02
    maxUrateL6M_Bin_WOE       -0.305842   4.341612e-02
    maxUrateL3M_Bin_WOE       -0.226448   1.296710e-01
    avgUrateL6M_Bin_WOE       -0.722688   1.398968e-07
    increaseUrateL12M_Bin_WOE  0.060439   6.793129e-01
    maxPayL12M_Bin_WOE         0.033850   8.149090e-01
    intercept                 -1.806185   0.000000e+00
    
    变量 M0FreqL12M_Bin_WOE，M2FreqL12M_Bin_WOE，minPayL1M_Bin_WOE，maxPayL6M_Bin_WOE，minPayL6M_Bin_WOE，increaseUrateL12M_Bin_WOE，maxPayL12M_Bin_WOE
    的系数为正，需要单独检验
    '''

    sm.Logit(y, trainData['minPayL1M_Bin_WOE']).fit().params  # -1.004459
    sm.Logit(y, trainData['minPayL6M_Bin_WOE']).fit().params  # -0.810183
    sm.Logit(y, trainData['increaseUrateL12M_Bin_WOE']).fit().params  # -0.914707
    sm.Logit(y, trainData['maxPayL12M_Bin_WOE']).fit().params  # -1.248234
    sm.Logit(y, trainData['M0FreqL12M_Bin_WOE']).fit().params  # -1.219784
    sm.Logit(y, trainData['M2FreqL12M_Bin_WOE']).fit().params  # -1.522221
    sm.Logit(y, trainData['maxPayL6M_Bin_WOE']).fit().params  # -2.41543

    # 单独建立回归模型，系数为负，与预期相符，说明仍然存在多重共线性
    # 下一步，用GBDT跑出变量重要性，挑选出合适的变量
    clf = ensemble.GradientBoostingClassifier()
    gbdt_model = clf.fit(X, y)
    importace = gbdt_model.feature_importances_.tolist()
    featureImportance = zip(multi_analysis, importace)
    featureImportanceSorted = sorted(featureImportance, key=lambda k: k[1], reverse=True)

    # 先假定模型可以容纳5个特征，再逐步增加特征个数，直到有特征的系数为负，或者p值超过0.1
    n = 5
    featureSelected = [i[0] for i in featureImportanceSorted[:n]]
    X_train = X[featureSelected + ['intercept']]
    logit = sm.Logit(y, X_train)
    logit_result = logit.fit()
    pvalues = logit_result.pvalues
    params = logit_result.params
    fit_result = pd.concat([params, pvalues], axis=1)
    fit_result.columns = ['coef', 'p-value']
    while (n < len(featureImportanceSorted)):
        nextVar = featureImportanceSorted[n][0]
        featureSelected = featureSelected + [nextVar]
        X_train = X[featureSelected + ['intercept']]
        logit = sm.Logit(y, X_train)
        logit_result = logit.fit()
        params = logit_result.params
        if max(params) < 0:
            n += 1
            continue
        else:
            featureSelected.remove(nextVar)
            n += 1

    pvalues = logit_result.pvalues
    params = logit_result.params
    fit_result = pd.concat([params, pvalues], axis=1)
    fit_result.columns = ['coef', 'p-value']

    '''
                                  coef        p-value
    maxDelqL3M_Bin_WOE       -0.808065  3.406139e-243
    avgPayL3M_Bin_WOE        -0.420803   1.579495e-37
    increaseUrateL6M_Bin_WOE -1.397993  3.239780e-117
    avgUrateL1M_Bin_WOE      -0.620077   8.324553e-10
    M2FreqL3M_Bin_WOE        -0.678802   2.568740e-63
    M1FreqL6M_Bin_WOE        -0.059585   1.184051e-01
    maxDelqL1M_Bin_WOE       -0.064350   2.236463e-02
    maxUrateL3M_Bin_WOE      -0.231349   1.215939e-01
    avgPayL6M_Bin_WOE        -0.114443   1.554420e-02
    avgUrateL3M_Bin_WOE      -0.302011   1.431962e-02
    avgPayL12M_Bin_WOE       -0.002649   9.651786e-01
    avgUrateL6M_Bin_WOE      -0.707645   2.335335e-07
    maxUrateL6M_Bin_WOE      -0.307528   4.209419e-02
    M1FreqL12M_Bin_WOE       -0.109063   4.286404e-03
    M0FreqL6M_Bin_WOE        -0.018139   6.401932e-01
    intercept                -1.801620   0.000000e+00
    
    M1FreqL6M_Bin_WOE, maxUrateL3M_Bin_WOE,M0FreqL6M_Bin_WOE和avgPayL12M_Bin_WOE的p值小于0.1.依次剔除两个变量
    剔除顺序是按照p值降序。直到剩下的变量的p值全部小于0.1
    '''
    largePValueVars = ['avgPayL12M_Bin_WOE', 'M0FreqL6M_Bin_WOE', 'maxUrateL3M_Bin_WOE', 'M1FreqL6M_Bin_WOE']
    for var in largePValueVars:
        featureSelected.remove(var)
        X_train = X[featureSelected + ['intercept']]
        logit = sm.Logit(y, X_train)
        logit_result = logit.fit()
        pvalues = logit_result.pvalues
        if max(pvalues) <= 0.1:
            break

    X_train = X[featureSelected + ['intercept']]
    logit = sm.Logit(y, X_train)
    logit_result = logit.fit()
    pvalues = logit_result.pvalues
    params = logit_result.params
    fit_result = pd.concat([params, pvalues], axis=1)
    fit_result.columns = ['coef', 'p-value']
    '''
                                  coef        p-value
    maxDelqL3M_Bin_WOE       -0.804901  4.441583e-244
    avgPayL3M_Bin_WOE        -0.420981   1.534747e-38
    increaseUrateL6M_Bin_WOE -1.401236  3.708757e-118
    avgUrateL1M_Bin_WOE      -0.665617   7.432589e-12
    M2FreqL3M_Bin_WOE        -0.680432   1.248162e-63
    M1FreqL6M_Bin_WOE        -0.060098   9.729852e-02
    maxDelqL1M_Bin_WOE       -0.064856   2.100560e-02
    avgPayL6M_Bin_WOE        -0.121531   2.997358e-03
    avgUrateL3M_Bin_WOE      -0.306087   1.296783e-02
    avgUrateL6M_Bin_WOE      -0.713439   1.754651e-07
    maxUrateL6M_Bin_WOE      -0.403431   3.570334e-03
    M1FreqL12M_Bin_WOE       -0.105167   5.370001e-03
    intercept                -1.800796   0.000000e+00
    '''
    # 此时所有特征的系数为负，p值小于0.1
    coef_dict = params.to_dict()

    trainData['log_odds'] = trainData.apply(lambda x: Predict_LR(x, finalFeatures, coef_dict), axis=1)
    perf_model = KS_AR(trainData, 'log_odds', 'label')
    # {'KS': 0.63429631484909543, 'AR': 0.67234042261989702}


    ###################################
    #   5，在测试集上测试逻辑回归的结果   #
    ###################################
    # 准备WOE编码后的变量
    modelFeatures = fit_result.index.tolist()
    modelFeatures.remove('intercept')
    modelFeatures = [i.replace('_Bin', '').replace('_WOE', '') for i in modelFeatures]

    numFeatures = [i for i in modelFeatures if i in numericalFeatures]
    charFeatures = [i for i in modelFeatures if i in categoricalFeatures]

    testData['maxDelqL1M'] = testData.apply(lambda x: DelqFeatures(x, 1, 'max delq'), axis=1)
    testData['maxDelqL3M'] = testData.apply(lambda x: DelqFeatures(x, 3, 'max delq'), axis=1)
    testData['M2FreqL3M'] = testData.apply(lambda x: DelqFeatures(x, 3, 'M2 times'), axis=1)
    testData['M1FreqL6M'] = testData.apply(lambda x: DelqFeatures(x, 6, 'M1 times'), axis=1)
    testData['M1FreqL12M'] = testData.apply(lambda x: DelqFeatures(x, 12, 'M1 times'), axis=1)
    testData['maxUrateL6M'] = testData.apply(lambda x: UrateFeatures(x, 6, 'max utilization rate'), axis=1)
    testData['avgUrateL1M'] = testData.apply(lambda x: UrateFeatures(x, 1, 'mean utilization rate'), axis=1)
    testData['avgUrateL3M'] = testData.apply(lambda x: UrateFeatures(x, 3, 'mean utilization rate'), axis=1)
    testData['avgUrateL6M'] = testData.apply(lambda x: UrateFeatures(x, 6, 'mean utilization rate'), axis=1)
    testData['increaseUrateL6M'] = testData.apply(lambda x: UrateFeatures(x, 6, 'increase utilization rate'), axis=1)
    testData['avgPayL3M'] = testData.apply(lambda x: PaymentFeatures(x, 3, 'mean payment ratio'), axis=1)
    testData['avgPayL6M'] = testData.apply(lambda x: PaymentFeatures(x, 6, 'mean payment ratio'), axis=1)

    testData['M2FreqL3M_Bin'] = testData['M2FreqL3M'].apply(lambda x: int(x >= 1))
    testData['maxDelqL1M_Bin'] = testData['maxDelqL1M'].apply(lambda x: MergeByCondition(x, ['==0', '==1', '>=2']))
    testData['maxDelqL3M_Bin'] = testData['maxDelqL3M'].apply(lambda x: MergeByCondition(x, ['==0', '==1', '>=2']))
    for var in modelFeatures:
        if var not in ['M2FreqL3M', 'maxDelqL1M', 'maxDelqL3M']:
            newBin = var + "_Bin"
            bin = [i.values() for i in bin_dict if var in i][0][0]
            testData[newBin] = testData[var].apply(lambda x: AssignBin(x, bin))

    finalFeatures = []
    for var in modelFeatures:
        var1 = var + '_Bin'
        var2 = var1 + "_WOE"
        finalFeatures.append(var2)
        testData[var2] = testData[var1].apply(lambda x: WOE_IV_dict[var1]['WOE'][x])

    testData['log_odds'] = testData.apply(lambda x: Predict_LR(x, finalFeatures, coef_dict), axis=1)

    perf_model = KS_AR(testData, 'log_odds', 'label')

    # KS＝64.49%， AR ＝ 68.64%，都高于30%的标准。因此该模型是可用的。

    ##########################
    #   6，在测试集上计算分数   #
    ##########################
    BasePoint, PDO = 500, 50
    testData['score'] = testData['log_odds'].apply(lambda x: BasePoint + PDO / np.log(2) * (-x))
    plt.hist(testData['score'])
