# -*- coding: utf-8 -*-
# @Time    : 2022-08-19 17:57
# @Author  : zhoujian.Pray
# @FileName: priorityParse.py
# @Software: PyCharm
import pandas as pd


def wei_data_parse(df_priority):
    '''
    渠道礼盒优先级清洗函数
    :param df_priority: DataFrame, 原渠道礼盒优先级数据
    :return: DataFrame, 返回预处理后的渠道礼盒优先级数据帧
    '''
    if df_priority.shape[0] == 0:
        df_priority = pd.DataFrame(columns=['bu', 'subChannel', 'productCode', 'productName', 'priority'])
    wei = pd.DataFrame(columns=['package', 'n', 'w'])
    for i in range(df_priority.shape[0]):
        wei = pd.concat([wei, pd.DataFrame(
            {'package': str(df_priority.loc[i, 'productCode']), 'n': str(df_priority.loc[i, 'subChannel']),
             'w': int(df_priority.loc[i, 'priority'])}, index=[0])], ignore_index=True)
    print('(priority)优先级权重数据{0}'.format(wei.head()))
    return wei
