# -*- coding: utf-8 -*-
# @Time    : 2022-08-19 17:28
# @Author  : zhoujian.Pray
# @FileName: ProducePlanParse.py
# @Software: PyCharm
import pandas as pd
import datetime

# 传统设置列名和列对齐
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


def lock_data_parse(df_last_produce, now_time):
    if df_last_produce.shape[0] == 0:
        df_last_produce = pd.DataFrame(columns=['scheduleId', 'productCode', 'bomVersion', 'subChannel', 'warehouse',
                                                'demandCommitDate', 'scheduleDate', 'planQuantity'])
    the_first_date = now_time + datetime.timedelta(days=1)
    # 1. 解析上一次排产计划（df_last_produce）
    '''
    输出lock=1的dataframe
    '''
    print('排产计划读取排产需求共有{0}行'.format(df_last_produce.shape[0]))
    df_last_produce["subChannel"] = df_last_produce["subChannel"].astype(str)  # 将subChannel列转化为字符串
    df_last_produce["productCode"] = df_last_produce["productCode"].astype(str)  # 将productCode列转化为字符串
    df_last_produce["bomVersion"] = df_last_produce["bomVersion"].astype(str)  # 将bomVersion列转化为字符串
    df_last_produce["scheduleId"] = df_last_produce["scheduleId"].astype(str)  # 将scheduleId列转化为字符串
    lock = pd.DataFrame(
        columns=['isLock', 'id', 'package', 'bom', 'n', 'warehouse', 's_t', 'o_t', 'num', 't'])  # 创建一个lock数据帧
    for i in range(df_last_produce.shape[0]):
        if (df_last_produce.loc[i, 'isLock']):
            report_date = datetime.datetime.strptime(df_last_produce.loc[i, 'demandCommitDate'],
                                                     "%Y-%m-%d").date()  # 提报日期
            produce_date = datetime.datetime.strptime(df_last_produce.loc[i, 'scheduleDate'], "%Y-%m-%d").date()  # 排产日期
            day_dis_report = (report_date - the_first_date).days  # s_t 提报间隔时间
            day_dis_produce = (produce_date - the_first_date).days  # o_t  排产间隔时间
            print('提报间隔时间：\n {0}--排产间隔时间：\n {1}'.format(day_dis_report, day_dis_produce))
            if (day_dis_report == 0):
                day_dis_report = day_dis_report + 1
            if (day_dis_produce >= 0):
                day_dis_produce = day_dis_produce + 1
                lock = lock.append(pd.DataFrame({'isLock': 1, 'id': df_last_produce.loc[i, 'scheduleId'],
                                                 'package': df_last_produce.loc[i, 'productCode'],
                                                 'bom': df_last_produce.loc[i, 'bomVersion'],
                                                 'n': df_last_produce.loc[i, 'subChannel'],
                                                 'warehouse': df_last_produce.loc[i, 'warehouse'],
                                                 's_t': int(day_dis_report),
                                                 'o_t': int(day_dis_produce),
                                                 'num': df_last_produce.loc[i, 'planQuantity'],
                                                 't': int(day_dis_produce)}, index=[0]), ignore_index=True)
    print('[1]: (lock)上一次的排产计划锁定数据：\n {0}'.format(lock.head()))
    return lock


def orders_f_data_parse(df_orders, lock, now_time):
    the_first_date = now_time + datetime.timedelta(days=1)
    # 2. 解析订单分装需求（df_orders）
    '''
    输出需求提报计划dataframe，同时和以往需求完成合并
    '''
    print('分装需求计划读取分装需求共有{0}行'.format(df_orders.shape[0]))
    df_orders["subChannel"] = df_orders["subChannel"].astype(str)  # 将subChannel列转化为字符串
    df_orders["giftBoxCode"] = df_orders["giftBoxCode"].astype(str)  # 将giftBoxCode列转化为字符串
    df_orders["bomVersion"] = df_orders["bomVersion"].astype(str)  # 将bomVersion列转化为字符串
    df_orders["packingPlanId"] = df_orders["packingPlanId"].astype(int)  # 将packingPlanId列转化为int
    df_orders["packingPlanSerialNum"] = df_orders["packingPlanSerialNum"].astype(int)
    df_orders["id"] = df_orders["packingPlanId"].astype(str) + "-" + df_orders["packingPlanSerialNum"].astype(str)
    data_orders = pd.DataFrame(
        columns=['id', 'package', 'bom', 'n', 'warehouse', 's_t', 'o_t', 'num'])  # 创建一个data_orders数据帧
    for i in range(df_orders.shape[0]):
        date_demand = datetime.datetime.strptime(df_orders.loc[i, 'packingPlanWeekNum'], "%Y-%m-%d").date()  # 预分装日期
        date_report = datetime.datetime.strptime(df_orders.loc[i, 'demandCommitDate'], "%Y-%m-%d").date()  # 提报日期
        dis_day = (date_demand - the_first_date).days  # 分装需求时间间隔（天） o_t
        tos_day = (date_report - the_first_date).days  # 提报日期时间间隔（天）s_t
        if dis_day >= 0:
            dis_day = dis_day + 1
            if dis_day >= 7:
                dis_day = 7
        if tos_day == 0:
            tos_day = tos_day + 1
        data_orders = data_orders.append(pd.DataFrame({'id': df_orders.loc[i, 'id'],
                                                       'package': df_orders.loc[i, 'giftBoxCode'],
                                                       'bom': df_orders.loc[i, 'bomVersion'],
                                                       'n': df_orders.loc[i, 'subChannel'],
                                                       'warehouse': df_orders.loc[i, 'warehouse'],
                                                       's_t': int(tos_day),
                                                       'o_t': int(dis_day),
                                                       'num': df_orders.loc[i, 'planPackingQuantity'],
                                                       }, index=[0]), ignore_index=True)

    print('(data_orders)需求提报计划锁定数据：\n {0}'.format(data_orders.head()))
    orders_f = pd.concat([lock, data_orders], ignore_index=True)
    orders_f['isLock'].fillna(value=0, inplace=True)
    print('[2]: (orders_f)需求提报计划锁定数据：\n {0}'.format(orders_f.head()))
    return orders_f, data_orders


def I_0_data_inventory_parse(df_inventory):
    # 3. 解析库存数据Inventory
    '''
    物料编码
    仓库编码
    库存数量
    '''
    # TODO(楊江南)：防止為空
    data_inventory = {'sample': df_inventory["productCode"],
                      'warehouse': df_inventory["warehouse"],
                      'num': df_inventory["stockNum"]}
    I_0 = pd.DataFrame(data_inventory)
    I_0["sample"] = I_0["sample"].astype(str)
    I_0["warehouse"] = I_0["warehouse"].astype(str)
    I_0["num"] = I_0["num"].astype(float)
    print('[3]: (I_0)在库库存数据：\n {0}'.format(I_0.head()))
    return I_0


def arr_data_parse(df_arrival, now_time, arrive_interval_days):
    '''
    解析到货数据arrival
    判别修改到货时间t,
    输出仓库信息，
    物料编码
    物料数量
    Add：arrive_interval_days 表示到货的间隔日期，如第一天到，则arrive_interval_days天后可用
    Add：available_day 表示排产时允许使用到货子件的相对日期（1、2、3、4）
    '''
    the_first_date = now_time + datetime.timedelta(days=1)
    arrive_interval_days = arrive_interval_days + 1  # 子件到货日期+1，用于转化为相对日期
    print('排产计划读取到货信息共有{0}行'.format(df_arrival.shape[0]))
    df_arrival["productCode"] = df_arrival["productCode"].astype(str)  # 将productCode列转化为string
    arr = pd.DataFrame(columns=['warehouse', 'sample', 'num', 't'])  # 防止arr空
    for i in range(df_arrival.shape[0]):
        date_arrival = datetime.datetime.strptime(df_arrival.loc[i, 'arrivalDate'], "%Y-%m-%d").date()
        dis_arr = (date_arrival - the_first_date).days + arrive_interval_days
        # 删除了使用到货的限定
        arr = pd.concat([arr, pd.DataFrame(
            {'warehouse': str(df_arrival.loc[i, 'warehouse']), 'sample': str(df_arrival.loc[i, 'productCode']),
             'num': float(df_arrival.loc[i, 'arrivalNum']), 't': int(dis_arr)}, index=[0])], ignore_index=True)
        '''
        arr = arr.append(pd.DataFrame({'warehouse': df_arrival["warehouse"],
                                       'sample': df_arrival["productCode"],
                                       'num': df_arrival["arrivalNum"],
                                       't': int(dis_arr)
                                       }, index=[0]), ignore_index=True)
        '''
    print('[4]: (arr)到货信息arr：{0}'.format(arr.head()))
    return arr
