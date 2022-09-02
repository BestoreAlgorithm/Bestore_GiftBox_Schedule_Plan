# -*- coding: utf-8 -*-
# @Time    : 2022-08-24 14:34
# @Author  : zhou_jian.Pray
# @FileName: PackPlanParse.py
# @Software: PyCharm

import pandas as pd
import datetime


def orders_data_parse(data_list):
    df_orders = pd.DataFrame(data_list["packingPlanInfo"])
    df_samples = pd.DataFrame(data_list["subSupplyPlanInfo"])
    return df_orders, df_samples


def data_orders_clean(df_orders, now_time, list_date):
    df_orders["packingPlanId"] = df_orders["packingPlanId"].astype(str)  # 将packingPlanId列转化为字符串
    df_orders["packingPlanSerialNum"] = df_orders["packingPlanSerialNum"].astype(int)  # 将packingPlanSerialNum列转化为整数
    df_orders["packingPlanVersion"] = df_orders["packingPlanVersion"].astype(str)  # 将packingPlanVersion列转化为字符串
    df_orders["subChannel"] = df_orders["subChannel"].astype(str)  # 将subChannel列转化为字符串
    df_orders["productCode"] = df_orders["productCode"].astype(str)  # 将productCode列转化为字符串
    df_orders["bomVersion"] = df_orders["bomVersion"].astype(str)  # 将bomVersion列转化为字符串
    df_orders["packingPlanId"] = df_orders["packingPlanId"].astype(str)  # 将packingPlanId列转化为字符串
    df_orders["id"] = df_orders["packingPlanId"].astype(str) + "-" + df_orders["packingPlanSerialNum"].astype(str)
    data_orders = pd.DataFrame(df_orders, columns=['id', 'productCode', 'bomVersion', 'subChannel',
                                                   'warehouse', 'demandCommitDate', 'packingPlanWeekNum',
                                                   'specifyScheduleIdentifier', 'planPackingQuantity'])
    data_orders['demandCommitDate'].replace('', "9999-12-31", inplace=True)  # 分装计划中提报时间预处理
    data_orders.rename(columns={'productCode': 'package', 'bomVersion': 'bom',
                                'subChannel': 'n', 'demandCommitDate': 's_t',
                                'packingPlanWeekNum': 'o_t', 'specifyScheduleIdentifier': 'flag',
                                'planPackingQuantity': 'num'}, inplace=True)
    data_orders['flag'] = data_orders['flag'].replace(["X", "x"], "1")
    data_orders.loc[data_orders.flag == "X", 'flag'] = "1"
    data_orders.loc[data_orders.flag == "x", 'flag'] = "1"
    data_orders.loc[(data_orders.flag != "x") & (data_orders.flag != "X"), 'flag'] = "0"
    data_orders['flag'] = data_orders['flag'].astype(int)
    the_first_week_date = datetime.datetime.strptime(list_date[0], "%Y-%m-%d").date()
    days_before_date = the_first_week_date + datetime.timedelta(days=-7)  # 第一周的前一周日期(特殊情况)
    days_before = days_before_date.strftime('%Y-%m-%d')  # str形式的日期
    for i in range(data_orders.shape[0]):  # 日期处理
        date_report = datetime.datetime.strptime(data_orders.loc[i, 's_t'], "%Y-%m-%d").date()  # 提报日期
        interval_day = (date_report - now_time).days
        if interval_day == 0:  # 防止优先级计算的时候报错 除0
            interval_day = 1
        data_orders.loc[i, 's_t'] = interval_day
    for i in range(data_orders.shape[0]):
        if data_orders.loc[i, 'o_t'] == days_before:
            data_orders.loc[i, 'o_t'] = -1
        else:
            for j in range(len(list_date)):
                if data_orders.loc[i, 'o_t'] == list_date[j]:
                    data_orders.loc[i, 'o_t'] = j + 1
                    break
    data_orders['s_t'] = data_orders['s_t'].astype(int)
    data_orders['o_t'] = data_orders['o_t'].astype(int)
    return data_orders


def I_0_data_clean(df_samples):
    data_inventory = pd.DataFrame(df_samples, columns=['subCode', 'factoryCode', 'currentStock'])
    data_inventory.rename(columns={'subCode': 'sample', 'factoryCode': 'warehouse', 'currentStock': 'num'},
                          inplace=True)
    I_0 = data_inventory.dropna()  # 删除含有空值的所有行
    I_0.reset_index(drop=True, inplace=True)
    I_0['sample'] = I_0['sample'].astype(str)
    return I_0


def trans_data_clean(df_samples, list_date_time):
    # TODO t为空值
    data_trans = pd.DataFrame(df_samples, columns=['subCode', 'factoryCode', 'appropriationPlanNum', 'planSupplyDate'])
    data_trans.rename(columns={'subCode': 'sample', 'factoryCode': 'warehouse', 'appropriationPlanNum': 'num',
                               'planSupplyDate': 't'}, inplace=True)
    trans = data_trans.dropna()
    trans.reset_index(drop=True, inplace=True)
    trans['sample'] = trans['sample'].astype(str)
    '''
    for i in range(trans.shape[0]):
        trans_date = datetime.datetime.strptime(trans.loc[i, 't'], "%Y-%m-%d").date()
        for j in range(len(list_date)):
            list_day = datetime.datetime.strptime(list_date[j], "%Y-%m-%d").date()
            dis_day = (list_day - trans_date).days
            if (dis_day >= 0 and dis_day < 7):
                trans.loc[i, 't'] = j + 1
                break
    '''
    for i in range(trans.shape[0]):
        trans_date = datetime.datetime.strptime(trans.loc[i, 't'], "%Y-%m-%d").date()  # 子件调拨到货日期
        the_first_week = list_date_time[0]
        tmp_dis_day = (trans_date - the_first_week).days  # 先与上一周的时间比较
        trans_dis_day = (tmp_dis_day - 1) // 7 + 2
        if trans_dis_day <= 0:
            trans_dis_day = trans_dis_day - 1
        trans.loc[i, 't'] = trans_dis_day
    trans['t'] = trans['t'].astype(int)
    return trans


def arrive_data_clean(df_samples, list_date_time):
    data_arrival = pd.DataFrame(df_samples, columns=['subCode', 'factoryCode', 'backToCargoPlanNum', 'planSupplyDate'])
    data_arrival.rename(columns={'subCode': 'sample', 'factoryCode': 'warehouse', 'backToCargoPlanNum': 'num',
                                 'planSupplyDate': 't'}, inplace=True)
    arr = data_arrival.dropna()
    arr.reset_index(drop=True, inplace=True)
    arr['sample'] = arr['sample'].astype(str)
    '''
    for i in range(arr.shape[0]):
        arr_date = datetime.datetime.strptime(arr.loc[i, 't'], "%Y-%m-%d").date()
        for j in range(len(list_date)):
            list_day = datetime.datetime.strptime(list_date[j], "%Y-%m-%d").date()
            dis_day = (list_day - arr_date).days
            if (dis_day >= 0 and dis_day < 7):
                arr.loc[i, 't'] = j + 1
                break
    '''
    for i in range(arr.shape[0]):
        arr_date = datetime.datetime.strptime(arr.loc[i, 't'], "%Y-%m-%d").date()
        the_first_week = list_date_time[0]
        tmp_dis_day = (arr_date - the_first_week).days
        arr_dis_day = (tmp_dis_day - 1) // 7 + 2
        if arr_dis_day <= 0:
            arr_dis_day = arr_dis_day - 1
        arr.loc[i, 't'] = arr_dis_day
    arr['t'] = arr['t'].astype(int)
    return arr

