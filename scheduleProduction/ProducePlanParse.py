# -*- coding: utf-8 -*-
# @Time    : 2022-08-19 17:28
# @Author  : zhoujian.Pray
# @FileName: ProducePlanParse.py
# @Software: PyCharm
import pandas as pd
import datetime
from scheduleProduction import model
import json
import sys
save_stdout = sys.stdout  # 保存当前控制台的输出路径

# 传统设置列名和列对齐
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


def lock_data_parse(df_last_produce, now_time, list_date):
    '''
    获取7天排产计划算法中, 上一次七天排产计划数据中仍然位于规划期内的锁定订单数据
    :param df_last_produce: DataFrame, 原上一七天排产计划数据
    :param now_time: datetime, 当前日期
    :param list_date: list(String), 规划日期的列表
    :return: DataFrame, 返回锁定标识为True且排产计划日期在日期列表中的锁定订单数据帧:
                 排产计划ID(合成), 成品编码, bom版本号, 子渠道编码, 仓库编码, 需求提报相对日期(int), 分装计划相对周次(int), 排产计划数量, 排产相对日期(int), 锁定标识(0,1)
    '''
    if df_last_produce.shape[0] == 0:
        df_last_produce = pd.DataFrame(columns=['packingPlanId', 'packingPlanSerialNum', 'scheduleId', 'productCode',
                                                'bomVersion', 'subChannel', 'warehouse', 'demandCommitDate',
                                                'scheduleDate', 'planQuantity'])
    df_last_produce['demandCommitDate'].replace('', list_date[6], inplace=True)  # 用于填充 提报日期 空白的订单，防止没有优先级
    df_last_produce['demandCommitDate'].fillna(list_date[6], inplace=True)  # 用于填充 提报日期 空白的订单，防止没有优先级
    df_last_produce.dropna(axis=0, subset=['scheduleId'], inplace=True)
    df_last_produce.reset_index(drop=True, inplace=True)
    df_last_produce.loc[:, 'scheduleId'] = df_last_produce['scheduleId'].astype(int)
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
        columns=['isLock', 'id', 'pack_id', 'package', 'bom', 'n', 'warehouse', 's_t', 'o_t', 'num', 't'])  # 创建一个lock数据帧
    for i in range(df_last_produce.shape[0]):
        if (df_last_produce.loc[i, 'isLock']):
            lock_order_id = str(df_last_produce.loc[i, 'packingPlanId']) + "-" +\
                            str(df_last_produce.loc[i, 'packingPlanSerialNum'])  # 分配一个分装单号，用于在分装单中减去已排产的数量，获得净需求
            report_date = datetime.datetime.strptime(df_last_produce.loc[i, 'demandCommitDate'],
                                                     "%Y-%m-%d").date()  # 提报日期
            produce_date = datetime.datetime.strptime(df_last_produce.loc[i, 'scheduleDate'], "%Y-%m-%d").date()  # 排产日期
            day_dis_report = (report_date - the_first_date).days  # s_t 提报间隔时间
            day_dis_produce = (produce_date - the_first_date).days  # o_t  排产间隔时间

            if (day_dis_report >= 0):
                day_dis_report = day_dis_report + 1
            if (day_dis_produce >= 0):
                day_dis_produce = day_dis_produce + 1
            lock = pd.concat([lock, pd.DataFrame({'isLock': 1,
                                                  'id': df_last_produce.loc[i, 'scheduleId'],
                                                  'pack_id': lock_order_id,
                                                  'package': df_last_produce.loc[i, 'productCode'],
                                                  'bom': df_last_produce.loc[i, 'bomVersion'],
                                                  'n': df_lastproduce.loc[i, 'subChannel'],
                                                  'warehouse': str(df_lastproduce.loc[i, 'warehouse']),
                                                  's_t': int(day_dis_report),
                                                  'o_t': int(day_dis_produce),
                                                  'num': int(df_lastproduce.loc[i, 'planQuantity']),
                                                  't': int(day_dis_produce)}, index=[0])], ignore_index=True)
    print('[1]: (lock)上一次的排产计划锁定数据：\n {0}'.format(lock.head()))
    return lock


def orders_f_data_parse(df_orders, lock, list_date):
    '''
    获取7天排产计划算法中锁定订单与近两周周度分装计划需求订单的并集数据帧, 与近两周周度分装计划需求订单的数据帧
    :param df_orders: DataFrame, 原近两周周度分装计划数据帧
    :param lock: DataFrame, 上一七天排产计划中锁定且有效的订单数据帧
    :param list_date: list['str'], 7天排产计划所需规划的日期列表
    :return: DataFrame, 返回锁定订单与近两周周度分装计划的并集数据帧:
                    订单ID(合成), 成品编码, bom版本号, 子渠道编码, 仓库编码, 需求提报相对日期(int), 分装计划相对周次(int), 成品需求数量, 锁定标识(0,1)
             DataFrame, 近两周周度分装计划数据帧:
                    分装计划ID(合成), 成品编码, bom版本号, 子渠道编码, 仓库编码, 需求提报相对日期(int), 分装计划相对周次(int), 分装计划数量
    Tip: 合并的数据帧列名称由于混合了分装计划和排产计划, 因此重新命名
    '''
    the_first_date = datetime.datetime.strptime(list_date[0], "%Y-%m-%d").date()
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
    df_orders['demandCommitDate'].replace('', list_date[6], inplace=True)
    df_orders['demandCommitDate'].fillna(list_date[6], inplace=True)
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
        else:
            dis_day = dis_day
        if tos_day >= 0:
            tos_day = tos_day + 1
        data_orders = pd.concat([data_orders, pd.DataFrame({'id': df_orders.loc[i, 'id'],
                                                            'package':df_orders.loc[i, 'giftBoxCode'],
                                                            'bom': df_orders.loc[i, 'bomVersion'],
                                                            'n': df_orders.loc[i, 'subChannel'],
                                                            'warehouse':df_orders.loc[i, 'warehouse'],
                                                            's_t':int(tos_day),
                                                            'o_t':int(dis_day),
                                                            'num':df_orders.loc[i, 'planPackingQuantity']
                                                            }, index=[0])], ignore_index=True)
    for i in range(data_orders.shape[0]):
        net_num = data_orders.loc[i, 'num']
        for j in range(lock.shape[0]):
            if data_orders.loc[i, 'id'] == lock.loc[j, 'pack_id']:
                net_num = net_num - lock.loc[j, 'num']
        data_orders.loc[i, 'num'] = net_num
    print('(data_orders)需求提报计划锁定数据：\n {0}'.format(data_orders.head()))
    orders_f = pd.concat([lock, data_orders], ignore_index=True)
    orders_f['isLock'].fillna(value=0, inplace=True)
    print('[2]: (orders_f)需求提报计划锁定数据：\n {0}'.format(orders_f.head()))
    return orders_f, data_orders


def I_0_data_inventory_parse(df_inventory):
    '''
    解析子件库存数据
    :param df_inventory: DataFrame, 原子件库存数据帧
    :return: DataFrame, 预处理后的子件库存数据帧:
              物料编码
              仓库编码
              库存数量
    '''
    # 3. 解析库存数据Inventory
    data_inventory = pd.DataFrame(df_inventory, columns=['productCode', 'warehouse', 'stockNum', 'subChannel'])
    I_0 = data_inventory.rename(columns={'productCode': 'sample',
                                         'warehouse': 'warehouse',
                                         'stockNum': 'num'})
    I_0["sample"] = I_0["sample"].astype(str)
    I_0["warehouse"] = I_0["warehouse"].astype(str)
    I_0["subChannel"] = I_0["subChannel"].astype(str)
    I_0["num"] = I_0["num"].astype(float)
    I_0 = I_0.groupby(['sample', 'warehouse', 'subChannel'], as_index=False).sum()
    print('[3]: (I_0)在库库存数据：\n {0}'.format(I_0.head()))
    return I_0


def arr_data_parse(df_arrival, now_time, arrive_interval_days, lock_days):
    '''
    解析子件到货数据arrival
    :param df_arrival: DataFrame, 原子件预约到货数据帧
    :param now_time: datetime, 当前日期
    :param arrive_interval_days: int, 表示到货的间隔日期，如第一天到，则arrive_interval_days天后可用
    :param lock_days: int, 表示锁定天数，用于锁定n天的排产和限定可以使用到货的日期
    :return: DataFrame, 返回清洗后的子件到货数据:
            仓库编码
            物料编码
            到货数量
            可使用日期
    '''
    the_first_date = now_time + datetime.timedelta(days=1)
    arrive_interval_days = arrive_interval_days + 1  # 子件到货日期+1，用于转化为相对日期
    print('排产计划读取到货信息共有{0}行'.format(df_arrival.shape[0]))
    # df_arrival["productCode"] = df_arrival["productCode"].astype(str)  # 将productCode列转化为string
    arr = pd.DataFrame(columns=['warehouse', 'sample', 'num', 'subChannel', 't'])  # 防止arr空
    for i in range(df_arrival.shape[0]):
        date_arrival = datetime.datetime.strptime(df_arrival.loc[i, 'arrivalDate'], "%Y-%m-%d").date()
        dis_arr = (date_arrival - the_first_date).days + arrive_interval_days
        # 新增了到货日期的归并
        if dis_arr <= 0:
            dis_arr = 1
        arr = pd.concat([arr, pd.DataFrame(
            {'warehouse': str(df_arrival.loc[i, 'warehouse']),
             'sample': str(df_arrival.loc[i, 'productCode']),
             'subChannel': str(df_arrival.loc[i, 'subChannel']),
             'num': float(df_arrival.loc[i, 'arrivalNum']),
             't': int(dis_arr)}, index=[0])], ignore_index=True)
    arr['t'] = arr['t'].map(lambda x: lock_days + 1 if x < lock_days + 1 else x)
    print('[4]: (arr)到货信息arr：{0}'.format(arr.head()))
    return arr


def Inventory_data_check(I_0, data_lock, pack_sample, Bom, request_id, ScheduleProductionResult):
    '''
    检查库存数据是否支持已锁定的排产订单
    :param I_0: DataFrame, 处理后的库存数据帧
    :param data_lock: DataFrame, 仅包含锁定订单的数据帧
    :param pack_sample: Dictionary, 键为礼盒成品编码, 值为所需子件编码List的字典
    :param Bom: DataFrame, 处理后的BOM单数据帧
    :param request_id: int, 传入的requestId
    :param ScheduleProductionResult: 合法的结果json文件存放的路径
    :return: 无返回值, 数据配套则继续，否则终止
    '''
    for line in range(data_lock.shape[0]):
        i = data_lock.loc[line, :]
        for s in pack_sample[i['package']]:
            if I_0[(I_0['sample'] == s) & (I_0['warehouse'] == i['warehouse']) & (I_0['subChannel'] ==
                                                                                      i['subChannel'])].shape[0] == 0:
                print("库存工厂{0}中的{1}渠道没有锁定排产单{2}所需的{3}子件库存数据信息".format(i['warehouse'], i['subChannel'], i['id'], s))
                res = {'code': 703,
                       'msg': "库存工厂{0}中的{1}渠道没有锁定排产单{2}所需的{3}子件库存数据".format(i['warehouse'], i['subChannel'], i['id'], s),
                       'requestId': request_id, 'data': []}
                with open(ScheduleProductionResult, 'w', encoding='utf-8') as write_f:
                    write_f.write(json.dumps(res, indent=4, ensure_ascii=False))
                print(True)
                sys.exit(0)
            else:
                index_temp = I_0[(I_0['sample'] == s) & (I_0['warehouse'] == i['warehouse']) &
                                 (I_0['subChannel'] == i['subChannel'])].index[0]
                if i['o_t'] < 0:  # 如果是锁定日期小于0(时间已过)，需要直接对库存进行减库存操作
                    inventory_copy = I_0  # 直接将输入的库存dataframe:I_0传递给新变量(新名称)，执行改操作
                else:
                    inventory_copy = I_0.copy()  # 否则浅复制，只用于判断，不用于值的修改
                inventory_copy.loc[index_temp, 'num'] = inventory_copy.loc[index_temp, 'num'] - i['num'] * model.get_bom(Bom, i['package'], s)
                if inventory_copy.loc[index_temp, 'num'] < 0:
                    print("库存工厂{0}中{1}渠道的{2}子件库存无法满足锁定排产单".format(i['warehouse'], i['subChannel'], s))
                    res = {'code': 704,
                           'msg': "库存工厂{0}中{1}渠道的{2}子件库存无法满足锁定排产单".format(i['warehouse'], i['subChannel'], s),
                           'requestId': request_id, 'data': []}
                    with open(ScheduleProductionResult, 'w', encoding='utf-8') as write_f:
                        write_f.write(json.dumps(res, indent=4, ensure_ascii=False))
                    sys.stdout = save_stdout
                    print(True)
                    sys.exit(0)
