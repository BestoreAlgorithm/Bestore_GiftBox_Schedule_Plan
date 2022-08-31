# -*- coding: utf-8 -*-
# @Time    : 2022-08-19 17:57
# @Author  : zhoujian.Pray
# @FileName: priorityParse.py
# @Software: PyCharm
import pandas as pd


def wei_data_parse(df_priority):
    if df_priority.shape[0] == 0:  # TODO(新增标记)
        df_priority = pd.DataFrame(columns=['bu', 'subChannel', 'productCode', 'productName', 'priority'])
    wei = pd.DataFrame(columns=['package', 'n', 'w'])
    for i in range(df_priority.shape[0]):
        wei = pd.concat([wei, pd.DataFrame(
            {'package': str(df_priority.loc[i, 'productCode']), 'n': str(df_priority.loc[i, 'subChannel']),
             'w': int(df_priority.loc[i, 'priority'])}, index=[0])], ignore_index=True)

    # df_priority['productCode'] = df_priority['productCode'].astype(str)
    # df_priority['subChannel'] = df_priority['subChannel'].astype(str)
    # data_priority = pd.DataFrame(df_priority, columns=['productCode', 'subChannel', 'priority'])
    # data_priority.rename(columns={'productCode': 'package',
    #                               'subChannel': 'n',
    #                               'priority': 'w'}, inplace=True)
    # wei = pd.DataFrame(data_priority)
    # TODO(改动标记）
        # 新代码如下，原代码是否支持优先级数据为空的情况？
    '''
    wei = pd.DataFrame(columns=['package', 'n', 'w'])
    for i in range(df_priority.shape[0]):
        wei = pd.concat([wei, pd.DataFrame(
            {'package': str(df_priority.loc[i, 'productCode']), 'n': str(df_priority.loc[i, 'subChannel']),
             'w': int(df_priority.loc[i, 'priority'])}, index=[0])], ignore_index=True)
    '''
    print('(priority)优先级权重数据{0}'.format(wei.head()))
    return wei
