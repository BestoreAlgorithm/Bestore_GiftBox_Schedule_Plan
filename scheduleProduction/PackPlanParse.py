# -*- coding: utf-8 -*-
# @Time    : 2022-08-24 14:34
# @Author  : zhou_jian.Pray
# @FileName: PackPlanParse.py
# @Software: PyCharm

import pandas as pd
import datetime


def orders_data_parse(data_list):
    '''
    13周主数据由字典转DataFrame函数, 分理出初始的周度分装计划和子件供应计划
    :param data_list: json主数据转化成的字典
    :return: DataFrame, 分装计划需求数据:
                  分装计划单号, 分装计划单序号, 分装计划版本, 事业部, 子渠道编码, 成品编码, 成品计划分装数量, bom版本号, 指定排期标识, 需求出库周, 分装计划周次, 需求提报日期, 锁定标识, 下单覆盖周次
             DataFrame, 子件供应计划数据
                  分装计划版本, 子件需求计划版本, 子件品类, 子件编码, 子件名称, 仓库编码, 子件当前库存数, 子件计划调拨数, 子件计划回货数, 计划供货日期
    '''
    df_orders = pd.DataFrame(data_list["packingPlanInfo"])
    df_samples = pd.DataFrame(data_list["subSupplyPlanInfo"])
    return df_orders, df_samples


def data_orders_clean(df_orders, now_time, list_date):
    '''
    13周分装计划需求数据清洗函数
    :param df_orders: DataFrame, 初始分装计划需求
    :param now_time: datetime, 当前日期
    :param list_date: list['str'], 需要进行分装计划的周次(周末)日期列表
    :return: DataFrame, 预处理后的分装计划需求数据:
                  分装计划ID(合成), 成品编码, bom版本号, 子渠道编码, 仓库编码, 需求提报相对日期(int), 分装计划相对周次(int), 指定排期标识(0,1), 分装计划数量
    '''
    df_orders["packingPlanId"] = df_orders["packingPlanId"].astype(str)  # 将packingPlanId列转化为字符串
    df_orders["packingPlanSerialNum"] = df_orders["packingPlanSerialNum"].astype(int)  # 将packingPlanSerialNum列转化为整数
    # TODO(修改输出的分装版本，去掉str转化)
    # df_orders["packingPlanVersion"] = df_orders["packingPlanVersion"].astype(str)  # 将packingPlanVersion列转化为字符串
    df_orders["subChannel"] = df_orders["subChannel"].astype(str)  # 将subChannel列转化为字符串
    df_orders["productCode"] = df_orders["productCode"].astype(str)  # 将productCode列转化为字符串
    df_orders["bomVersion"] = df_orders["bomVersion"].astype(str)  # 将bomVersion列转化为字符串
    df_orders["packingPlanId"] = df_orders["packingPlanId"].astype(str)  # 将packingPlanId列转化为字符串
    df_orders["id"] = df_orders["packingPlanId"].astype(str) + "-" + df_orders["packingPlanSerialNum"].astype(str)
    data_orders = pd.DataFrame(df_orders, columns=['id', 'productCode', 'bomVersion', 'subChannel',
                                                   'warehouse', 'demandCommitDate', 'packingPlanWeekNum',
                                                   'specifyScheduleIdentifier', 'planPackingQuantity'])
    data_orders['demandCommitDate'].replace('', list_date[12], inplace=True)  # 分装计划中提报时间预处理  # TODO(改动标记)
    data_orders['demandCommitDate'].fillna(list_date[12], inplace=True)  # 分装计划中提报时间预处理  # TODO(提报日期为空新增处理方案)
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
    '''
    子件库存数据获取函数
    :param df_samples: DataFrame, 原子件供应计划数据
    :return: DataFrame, 子件库存数据帧:
                物料编码
                仓库编码
                库存数量
    '''
    data_inventory = pd.DataFrame(df_samples, columns=['subCode', 'factoryCode', 'currentStock'])  # df_samples为空值时也支持
    data_inventory.rename(columns={'subCode': 'sample', 'factoryCode': 'warehouse', 'currentStock': 'num'},
                          inplace=True)
    I_0 = data_inventory.dropna()  # 删除含有空值的所有行
    I_0.reset_index(drop=True, inplace=True)
    I_0['sample'] = I_0['sample'].astype(str)
    return I_0


def trans_data_clean(df_samples, list_date_time):
    '''
    调拨子件数据获取函数
    :param df_samples: DataFrame, 原子件供应计划数据
    :param list_date_time: list['datetime'], 需要进行分装计划的周次(周末)日期datetime列表
    :return: DataFrame, 调拨子件数据:
                物料编码
                仓库编码
                调拨数量
                到货周次(int, 相对时间节点)
    '''
    data_trans = pd.DataFrame(df_samples, columns=['subCode', 'factoryCode', 'appropriationPlanNum', 'planSupplyDate'])
    data_trans.rename(columns={'subCode': 'sample', 'factoryCode': 'warehouse', 'appropriationPlanNum': 'num',
                               'planSupplyDate': 't'}, inplace=True)
    trans = data_trans.dropna()
    trans.reset_index(drop=True, inplace=True)
    trans['sample'] = trans['sample'].astype(str)
    for i in range(trans.shape[0]):
        trans_date = datetime.datetime.strptime(trans.loc[i, 't'], "%Y-%m-%d").date()  # 子件调拨到货日期
        the_first_week = list_date_time[0]
        tmp_dis_day = (trans_date - the_first_week).days  # 先与上一周的时间比较
        trans_dis_day = (tmp_dis_day - 1) // 7 + 3
        if trans_dis_day <= 0:
            trans_dis_day = 1
        trans.loc[i, 't'] = trans_dis_day
    trans['t'] = trans['t'].astype(int)
    return trans


def arrive_data_clean(df_samples, list_date_time):
    '''
    子件预约到货数据获取函数
    :param df_samples: DataFrame, 原子件供应计划
    :param list_date_time: list['datetime'], 需要进行分装计划的周次(周末)日期datetime列表
    :return: DataFrame, 子件预约到货数据:
                物料编码
                仓库编码
                到货数量
                到货周次(int, 相对时间节点)
    '''
    data_arrival = pd.DataFrame(df_samples, columns=['subCode', 'factoryCode', 'backToCargoPlanNum', 'planSupplyDate'])
    data_arrival.rename(columns={'subCode': 'sample', 'factoryCode': 'warehouse', 'backToCargoPlanNum': 'num',
                                 'planSupplyDate': 't'}, inplace=True)
    arr = data_arrival.dropna()
    arr.reset_index(drop=True, inplace=True)
    arr['sample'] = arr['sample'].astype(str)
    for i in range(arr.shape[0]):
        arr_date = datetime.datetime.strptime(arr.loc[i, 't'], "%Y-%m-%d").date()
        the_first_week = list_date_time[0]
        tmp_dis_day = (arr_date - the_first_week).days
        arr_dis_day = (tmp_dis_day - 1) // 7 + 3
        if arr_dis_day <= 0:
            arr_dis_day = 1
        arr.loc[i, 't'] = arr_dis_day
    arr['t'] = arr['t'].astype(int)
    return arr


def Cover_Add_num(df_orders_original, now_time, list_date_time):
    '''
    获取下单覆盖周次附加系数
    Tip：测试阶段使用, 算法上线后可去除。覆盖周次基准周由当前周(周末日期)转变为最晚提报日期所在周(周末日期)后, 实际下单覆盖周次数
    不再为传入的覆盖周次数(coverageWeekNum), 需要进行换算, 返回的覆盖周次附加系数为换算系数, 大多情况下为负。
    :param df_orders_original: DataFrame, 没有经过任何处理的原分装计划数据
    :param now_time: datetime, 当前日期
    :param list_date_time: list['datetime'], 需要进行分装计划的周次(周末)日期datetime列表
    :return: int, 用于换算覆盖周次数的附加系数
    '''
    T_cover_add_num = 0
    list_report_time = []
    for i in range(df_orders_original.shape[0]):
        if df_orders_original.loc[i, 'demandCommitDate'] is not None and df_orders_original.loc[i, 'demandCommitDate'] != '':
            list_report_time.append(df_orders_original.loc[i, 'demandCommitDate'])
    list_report_date_num = []
    for i in range(len(list_report_time)):
        report_time_date = datetime.datetime.strptime(list_report_time[i], "%Y-%m-%d").date()  # 子件调拨到货日期
        report_time_num = (report_time_date - now_time).days
        list_report_date_num.append(report_time_num)
    max_report_date_num = max(list_report_date_num)
    max_report_date = now_time + datetime.timedelta(days=max_report_date_num)
    max_report_weekday = max_report_date.weekday() + 1
    max_report_this_week = max_report_date + datetime.timedelta(days=7 - max_report_weekday)
    T_cover_add_num = (max_report_this_week - list_date_time[0]).days // 7  # 所要求得的内容
    return T_cover_add_num
