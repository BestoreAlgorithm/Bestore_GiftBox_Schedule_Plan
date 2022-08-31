# -*- coding: utf-8 -*-
# @Time    : 2022-08-24 11:21
# @Author  : zhou_jian.Pray
# @FileName: 13weekRestructure.py
# @Software: PyCharm
from ortools.linear_solver import pywraplp
from gurobipy import tupledict
import pandas as pd
import time
import json
import datetime
import os
import itertools
from scheduleProduction import shareFunction
from scheduleProduction import bomsParse
from scheduleProduction import capacityParse
from scheduleProduction import priorityParse
from scheduleProduction import PackPlanParse
from scheduleProduction import model

# 传统设置列名和列对齐
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
# 全局变量
'''
1. 这个地方需要修改文件夹的路径的转义符
2. 变量命名的定义：大小写问题(ProducePlan, capacity)

'''
data_prepare_start = time.time()  # 数据准备时间开始函数

# 设置数据路径全局变量
project_path = os.getcwd()  # 当前路径
jsons_filename = 'jsons'  # 存放数据的文件夹的名称
jsons_data_path = project_path + '\\' + jsons_filename  # 原始数据的路径
ProducePlan = jsons_data_path + '\\' + 'ProducePlan.json'
PackPlan = jsons_data_path + '\\' + 'PackPlan.json'
Bom = jsons_data_path + '\\' + 'Bom.json'
Capacity = jsons_data_path + '\\' + 'Capacity.json'
Priority = jsons_data_path + '\\' + 'Priority.json'
Calendar = jsons_data_path + '\\' + 'Calendar.json'  # TODO (新增标记)
print('json数据所在的文件夹路径：', jsons_data_path)
print('排产计划数据所在的文件夹路径：', ProducePlan)
print('Bom主数据所在的文件夹路径：', Bom)
print('产能主数据所在的文件夹路径：', Capacity)
print('优先级主数据所在的文件夹路径：', Priority)
print('日历主数据所在的文件路径：', Calendar)  # TODO（新增标记）

now_time = datetime.date.today()  # 当日日期
# 日期处理：生成13周的str日期和date日期
list_date_time, list_date = shareFunction.data_list_create(13, now_time, 13)
# TODO（新增标记）
days_before_date = list_date_time[0] + datetime.timedelta(days=-7)  # 第一周的前一周日期(特殊情况)
days_before = days_before_date.strftime('%Y-%m-%d')  # str形式的日期

# 解析PackPlan.json数据
with open(PackPlan, "r", encoding="utf-8") as f_json:
    info = f_json.read()
    data_list = json.loads(info)
df_orders, df_samples = PackPlanParse.orders_data_parse(data_list)
print('df_orders:\n {0}\n df_samples:\n {1}'.format(df_orders.head(), df_samples.head()))
print('df_orders_type:\n {0}'.format(df_orders.dtypes))

# 从df_orders中清洗出Order
Order = PackPlanParse.data_orders_clean(df_orders, now_time, list_date)
print('Order:\n {}\n Order:\n {}'.format(Order.head(), Order.dtypes))

df_samples.fillna(0, inplace=True)  # 子件供应计划预处理 TODO(新增标记)

# inventory
InventoryInitial = PackPlanParse.I_0_data_clean(df_samples)  # 解析库存数据Inventory
print('InventoryInitial:\n {}\n InventoryInitial_type:\n {}'.format(InventoryInitial.head(), InventoryInitial.dtypes))
Trans = PackPlanParse.trans_data_clean(df_samples, list_date_time)  # TODO(改动标记)
print('Trans:\n {}\n Trans_type:\n {}'.format(Trans.head(), Trans.dtypes))
Arr = PackPlanParse.arrive_data_clean(df_samples, list_date_time)  # 解析到货信息  # TODO(改动标记)
print('Arr:\n {}\n Arr_type:\n {}'.format(Arr.head(), Arr.dtypes))

# BOM基础数据json信息读入与解析
with open(Bom, "r", encoding="utf-8") as f_json_bom:
    info_bom = f_json_bom.read()
    data_list_bom = json.loads(info_bom)
    df_bom = pd.DataFrame(data_list_bom)
# TODO 这个地方day 和 week的输入参数不同，可以重构 目前了 bomsParse文件中
BOM = bomsParse.bom_data_parse(df_bom, Order)  # TODO(改动标记)
print('BOM:\n {}\n BOM_type:\n {}'.format(BOM.head(), BOM.dtypes))

# capacity基础数据分装产能读入与解析
# TODO(新增标记)
with open(Calendar, 'r', encoding='UTF-8') as cal_f:
    calendar_list = json.load(cal_f)

# TODO(改动标记)
with open(Capacity, "r", encoding="utf-8") as f_json_capacity:
    info_capacity = f_json_capacity.read()
    data_list_capacity = json.loads(info_capacity)
    df_capacity = pd.DataFrame(data_list_capacity)
PackingCapacity = capacityParse.pc_data_parse(13, df_capacity, calendar_list, list_date)

# priority优先级json信息读入与解析
# TODO(改动标记)
with open(Priority, "r", encoding="utf-8") as f_json_priority:
    info_priority = f_json_priority.read()
    data_list_priority = json.loads(info_priority)
    df_priority = pd.DataFrame(data_list_priority)
Wei = priorityParse.wei_data_parse(df_priority)

sample_data = model.get_sample(Order, BOM, 13)
print('sample_data:\n {}\n sample_data_type:\n {}'.format(sample_data.head(), sample_data.dtypes))

# TODO 变量含义待确定
# parameter
COVER_NUM = int(df_orders.loc[0, 'coverageWeekNum']) - 1
PACK_RANGE = 14
T = range(1, PACK_RANGE + 1)

# lists
print('Order:\n{0}\n Order_type:\n {1}'.format(Order.head(), Order.dtypes))
N = list(Order['n'].unique())  # 渠道列表
WAREHOUSE = list(Order['warehouse'].unique())  # warehouse list
PACKAGE = list(Order['package'].unique())  # package list ‘
ORDER_ID = list(Order['id'].unique())  # id list ’
SAMPLE = list(sample_data.unique())  # Sample list

X_INDEX, X1_INDEX = model.get_xindex_x1index(ORDER_ID, Order, T, PACK_RANGE, COVER_NUM)
print('X1_INDEX: \n {0}, \n X_INDEX: \n {1}'.format(X1_INDEX, X_INDEX))
PackSample = model.get_package_sample(BOM, PACKAGE)

# 新增
Y_INDEX = model.get_yindex(X_INDEX, ORDER_ID, Order, PackSample)
print('Y_INDEX: \n {}'.format(Y_INDEX))

data_prepare_end = time.time()  # 数据准备时间结束函数
print('parameters are ready, time cost are: {0}s'.format(str(data_prepare_end - data_prepare_start)))

# 2） 模型变量创建
model_train_start = time.time()  # 模型训练开始时间函数
# 声明求解器(实例化求解器)
solver = pywraplp.Solver.CreateSolver('SCIP')  # 创建一个基于GLOP后端的线性求解器
if not solver:
    print('solver setup failed!!')

INFINITY = solver.infinity()
# 初始化[x]tupledict变量子集
x = model.create_x_tupledict(ORDER_ID, X_INDEX, solver, INFINITY, PACK_RANGE)
# 创建[y]tupledict变量子集
y = model.create_y_tupledict(ORDER_ID, Y_INDEX, solver, INFINITY)
# 创建[invent]tupledict变量子集
invent = model.create_invent_tupledict(WAREHOUSE, SAMPLE, T, solver, INFINITY)
# 初始化[x_1]tupledict变量子集
x_1 = model.create_x_1_tupledict(ORDER_ID, X1_INDEX, solver, INFINITY, PACK_RANGE)
# 初始化[x_2]tupledict变量子集
x_2 = model.create_x_2_tupledict(ORDER_ID, X_INDEX, solver, INFINITY, PACK_RANGE)

# 3) 设置求解器约束
# 添加x变量的锁定库存的约束
# 添加库存约束(1)

for k, s in itertools.product(WAREHOUSE, SAMPLE):
    x_sum = 0
    for i_d in ORDER_ID:
        for i in X_INDEX[i_d]:
            if k == i['k'] and (i['t'] == 1 or i['t'] == -1):
                if s in PackSample[model.id2p(i_d, Order)]:
                    bom_nums = model.get_bom(BOM, i['m'], s)
                    x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']] * bom_nums
    solver.Add(x_sum + invent[k, s, 1] ==
               model.get_inventory(InventoryInitial, k, s) +
               model.get_trans(Trans, k, s, 1) +
               model.get_arr(Arr, k, s, 1))

# 添加库存约束(2)
for k, s, t in itertools.product(WAREHOUSE, SAMPLE, range(2, COVER_NUM + 1)):
    x_sum = 0
    for i_d in ORDER_ID:
        for i in X_INDEX[i_d]:
            if k == i['k'] and t == i['t']:
                if s in PackSample[model.id2p(i_d, Order)]:
                    bom_nums = model.get_bom(BOM, i['m'], s)
                    # print('bom_nums: {0}\n, x: {1}'.format(bom_nums, type(bom_nums)))
                    x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']] * bom_nums
    solver.Add(x_sum + invent[k, s, t] ==
               invent[k, s, t - 1] +
               model.get_trans(Trans, k, s, t) +
               model.get_arr(Arr, k, s, t))

# 添加库存约束(3)
for k, s, t in itertools.product(WAREHOUSE, SAMPLE, range(COVER_NUM + 1, T[-1] + 1)):
    x_sum = 0
    y_sum = 0
    for i_d in ORDER_ID:
        for i in X_INDEX[i_d]:
            if k == i['k'] and t == i['t']:
                if s in PackSample[model.id2p(i_d, Order)]:
                    bom_nums = model.get_bom(BOM, i['m'], s)
                    # print('bom_nums: {0}\n, x: {1}'.format(bom_nums, type(bom_nums)))
                    x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']] * bom_nums
        for j in Y_INDEX[i_d]:
            if k == j['k'] and s == j['s'] and t == j['t']:
                y_sum = y_sum + y[j['s'], j['id'], j['m'], j['n'], j['k'], j['s_t'], j['o_t'], j['f'], j['t']]
    solver.Add(x_sum + invent[k, s, t] == invent[k, s, t - 1] + y_sum)

# 添加需求约束
for i_d in ORDER_ID:
    for i in X1_INDEX[i_d]:
        x_sum = 0
        if i['o_t'] <= COVER_NUM:
            for pt in model.sum_range(0, i['o_t'] - 2, i['o_t'] + 1):
                x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], pt]
            solver.Add(x_sum + x_1[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f']] ==
                       model.get_demand(Order, i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f']))
        else:
            for pt in model.sum_range(COVER_NUM, i['o_t'] - 2, i['o_t'] + 1):
                x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], pt]
            solver.Add(x_sum + x_1[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f']] ==
                       model.get_demand(Order, i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f']))

for i_d in ORDER_ID:
    for i in X_INDEX[i_d]:
        if i['o_t'] == -1:
            for pt in range(1, PACK_RANGE + 1):
                x_sum = 0
                for t_sum in range(1, pt + 1):
                    x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], t_sum]
                solver.Add(x_sum + x_2[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], pt] ==
                           model.get_demand(Order, i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f']))
        else:
            if i['t'] >= i['o_t']:
                x_sum = 0
                for t_sum in range(list(model.packing_t(i['o_t'], COVER_NUM, T))[0], i['t'] + 1):
                    x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], t_sum]
                solver.Add(x_sum + x_2[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']] ==
                           model.get_demand(Order, i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f']))

# 供应量约束
# TODO supply_c这个函数明显有问题
for s, t in itertools.product(SAMPLE, T):
    y_sum = 0
    for i_d in ORDER_ID:
        for i in Y_INDEX[i_d]:
            if i['s'] == s and i['t'] == t:
                y_sum = y_sum + y[i['s'], i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']]
    solver.Add(y_sum <= model.supply_c(s, t))

# 添加产能约束
for k, t in itertools.product(WAREHOUSE, T):
    rate = 0
    for i_d in ORDER_ID:
        for i in X_INDEX[i_d]:
            if i['k'] == k and i['t'] == t:
                rate = rate + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']] / \
                       model.warehouse_s(PackingCapacity, k, i['m'])
    solver.Add(rate <= float(model.warehouse_c(PackingCapacity, k, t)))

# 4) 设置目标函数
delaySum = 0
for i_d in ORDER_ID:
    for i in X1_INDEX[i_d]:
        delaySum = delaySum + model.type_weight(i['m'], i['n'], i['k'], i['s_t'], i['o_t'], Wei, 1, i['f']) * \
                   x_1[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f']]

loseSum = 0
for i_d in ORDER_ID:
    for i in X_INDEX[i_d]:
        if i['t'] > i['o_t']:
            loseSum = loseSum + model.type_weight(i['m'], i['n'], i['k'], i['s_t'], i['o_t'], Wei, 2, i['f']) * \
                      x_2[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']]
y_sum = 0
for i_d in ORDER_ID:
    for i in Y_INDEX[i_d]:
        y_sum = y_sum + y[i['s'], i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']] * \
                (PACK_RANGE - i['t'])

h_c = 0
for t in T:
    h_t = 0
    for i_d in ORDER_ID:
        for i in X_INDEX[i_d]:
            if i['t'] <= t:
                h_t = h_t + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']]
    h_c = h_c + h_t

solver.Minimize(delaySum + loseSum + 0.01 * y_sum + 0.001 * h_c)
print('Objective function setting done ！')

# 5）开始训练
print('Optimizing...')
status = solver.Solve()
if status == pywraplp.Solver.OPTIMAL:
    model_train_end = time.time()  # # 模型训练结束时间函数
    print('Optimal solution are ready, time cost are: {0}s'.format(str(model_train_end - model_train_start)))
else:
    print('The problem does not have an optimal solution.')

y_demand = tupledict()
request_id = data_list['requestId']  # TODO(新增标记)
res = {'code': 200, 'msg': "success", 'data': {'packingPlanInfo': [], 'subSupplyPlanInfo': [], 'requestId': request_id}}
for i_d in ORDER_ID:
    for i in X_INDEX[i_d]:
        if x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']].solution_value() > 0:
            print((i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']), x[i['id'], i['m'], i['n'],
                                                                                           i['k'], i['s_t'], i['o_t'],
                                                                                           i['f'], i[
                                                                                               't']].solution_value())
            if i['t'] == -1:
                real_pack_t = days_before  # TODO(新增标记)
            elif i['t'] == 14:
                real_pack_t_time = list_date_time[12] + datetime.timedelta(days=7)
                real_pack_t = real_pack_t_time.strftime('%Y-%m-%d')
            else:
                real_pack_t = list_date[i['t'] - 1]
            for od in range(Order.shape[0]):
                if i['id'] == Order.loc[od, 'id']:  # TODO(改动标记)
                    if df_bom[(df_bom["productCode"] == i['m']) &
                              (df_bom["bomVersion"] == Order.loc[od, 'bom'])]["productName"].shape[0] == 0:
                        product_name = "null"
                    else:
                        product_name = \
                            df_bom[(df_bom["productCode"] == i['m']) & (df_bom["bomVersion"] == Order.loc[od, 'bom'])][
                                "productName"].values[0]
                    mid_res = {'packingPlanId': str(df_orders.loc[od, 'packingPlanId']),
                               'packingPlanSerialNum': int(df_orders.loc[od, 'packingPlanSerialNum']),
                               'oldPackingPlanVersion': str(df_orders.loc[od, 'packingPlanVersion']),
                               'demandCommitDate': df_orders.loc[od, 'demandCommitDate'],
                               'requireOutWeek': df_orders.loc[od, 'requireOutWeek'],
                               'oldPackingPlanWeekNum': df_orders.loc[od, 'packingPlanWeekNum'],
                               'packingPlanWeekNum': real_pack_t,  # TODO(改变标记)
                               'bu': df_orders.loc[od, 'bu'],
                               'subChannel': df_orders.loc[od, 'subChannel'],
                               'warehouse': df_orders.loc[od, 'warehouse'],
                               'productCode': df_orders.loc[od, 'productCode'],
                               'productName': product_name,
                               'bomVersion': str(df_orders.loc[od, 'bomVersion']),
                               'oldPlanPackingQuantity': int(df_orders.loc[od, 'planPackingQuantity']),
                               'planPackingQuantity': int(x[i['id'], i['m'], i['n'], i['k'], i['s_t'],
                                                            i['o_t'], i['f'], i['t']].solution_value()),
                               'lockIdentifier': str(df_orders.loc[od, 'lockIdentifier'])}
                    res['data']['packingPlanInfo'].append(mid_res)
                    for s in PackSample[i['m']]:  # TODO(新增标记)
                        y_demand[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t'], s] = x[i['id'], i[
                            'm'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']].solution_value() * model.get_bom(
                            BOM, i['m'], s)
                        print((i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t'], s),
                              round(y_demand[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t'], s],
                                    3))
                        sub_name = df_bom[df_bom["subCode"] == str(s)]["subName"].values[0]
                        mid_sample = {'oldPackingPlanVersion': str(df_orders.loc[od, 'packingPlanVersion']),
                                      'packingPlanId': str(df_orders.loc[od, 'packingPlanId']),
                                      'packingPlanSerialNum': int(df_orders.loc[od, 'packingPlanSerialNum']),
                                      'demandCommitDate': df_orders.loc[od, 'demandCommitDate'],
                                      'requireOutWeek': df_orders.loc[od, 'requireOutWeek'],
                                      'packingPlanWeekNum': real_pack_t,
                                      'bu': df_orders.loc[od, 'bu'],
                                      'subChannel': str(df_orders.loc[od, 'subChannel']),
                                      'warehouse': df_orders.loc[od, 'warehouse'],
                                      'subCode': s,
                                      'subName': sub_name,
                                      'subDemandQuantity': round(y_demand[i['id'], i['m'], i['n'], i['k'], i['s_t'],
                                                                          i['o_t'], i['f'], i['t'], s], 3),
                                      'subDemandPlanDate': real_pack_t}
                        res['data']['subSupplyPlanInfo'].append(mid_sample)

'''
for i_d in p_id:
    for i in yIndex[i_d]:
        if y[i['s'], i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']].solution_value() > 0:
            print((i['s'], i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']),
                  y[i['s'], i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i['t']].solution_value())
            sub_name = df_bom[df_bom["subCode"] == str(i['s'])]["subName"].values[0]
            for od in range(Order.shape[0]):
                if i['id'] == Order.loc[od, 'id']:
                    mid_sample = {'oldPackingPlanVersion': str(df_orders.loc[od, 'packingPlanVersion']),
                                  'packingPlanId': str(df_orders.loc[od, 'packingPlanId']),
                                  'packingPlanSerialNum': int(df_orders.loc[od, 'packingPlanSerialNum']),
                                  'demandCommitDate': df_orders.loc[od, 'demandCommitDate'],
                                  'requireOutWeek': df_orders.loc[od, 'requireOutWeek'],
                                  'packingPlanWeekNum': df_orders.loc[od, 'packingPlanWeekNum'],
                                  'bu': df_orders.loc[od, 'bu'],
                                  'subChannel': str(df_orders.loc[od, 'subChannel']),
                                  'warehouse': df_orders.loc[od, 'warehouse'], 'subCode': i['s'], 'subName': sub_name,
                                  'subDemandQuantity': y[
                                      i['s'], i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['f'], i[
                                          't']].solution_value(),
                                  'subDemandPlanDate': list_date[i['t'] - 1]}
                    res['data']['subSupplyPlanInfo'].append(mid_sample)
                    check_null_flag = 1
if check_null_flag == 0:
    mid_sample = {'oldPackingPlanVersion': "null", 'packingPlanId': "null", 'packingPlanSerialNum': 0,
                  'demandCommitDate': "null", 'requireOutWeek': "null", 'packingPlanWeekNum': "null", 'bu': "null",
                  'subChannel': "null", 'warehouse': "null", 'subCode': "null", 'subName': "null",
                  'subDemandQuantity': 0, 'subDemandPlanDate': "null"}
    res['data']['subSupplyPlanInfo'].append(mid_sample)
'''

with open("13weeks_schedule_plan_result.json", 'w', encoding='utf-8') as write_f:
    write_f.write(json.dumps(res, indent=4, ensure_ascii=False))
