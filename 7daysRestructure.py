# -*- coding: utf-8 -*-
# @Time    : 2022-08-17 15:51
# @Author  : zhou_jian.Pray
# @FileName: 7daysRestructure.py
# @Software: PyCharm

# load library

'''ortools线性求解器，gurobipy文件'''
import pandas as pd
import time
import datetime
import json
import os
import itertools
import sys
from ortools.linear_solver import pywraplp
from scheduleProduction import shareFunction
from scheduleProduction import ProducePlanParse
from scheduleProduction import bomsParse
from scheduleProduction import capacityParse
from scheduleProduction import priorityParse
from scheduleProduction import model
from ortools.linear_solver.linear_solver_natural_api import SumArray
# 传统设置列名和列对齐
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
# 全局变量
data_prepare_start = time.time()  # 数据准备时间开始函数

# 设置数据路径全局变量


project_path = os.getcwd()  # 当前路径
jsons_filename = 'jsons'  # 存放数据的文件夹的名称
result_filename = 'result'  # 结果数据的文件夹的名称
jsons_data_path = project_path + '\\' + jsons_filename  # 原始数据的路径
result_data_path = project_path + '\\' + result_filename  # 结果数据的路径
ProducePlan = jsons_data_path + '\\' + 'ProducePlan.json'
Bom = jsons_data_path + '\\' + 'Bom.json'
Capacity = jsons_data_path + '\\' + 'Capacity.json'
Priority = jsons_data_path + '\\' + 'Priority.json'
Calendar = jsons_data_path + '\\' + 'Calendar.json'
ScheduleProductionResult = result_data_path + '\\' + '7days_schedule_plan_result.json'
ExecLog = result_data_path + '\\' + 'exec_7days.log'
save_stdout = sys.stdout  # 保存当前控制台的输出路径
sys.stdout = open(ExecLog, mode='w', encoding='utf-8')  # 添加log记录
print('json数据所在的文件夹路径：', jsons_data_path)
print('排产计划数据所在的文件夹路径：', ProducePlan)
print('Bom主数据所在的文件夹路径：', Bom)
print('产能主数据所在的文件夹路径：', Capacity)
print('优先级主数据所在的文件夹路径：', Priority)
print('日历主数据所在的文件路径：', Calendar)


# 日期变量
'''
1、创建日期列表（从今天到未来滚动七天）
2、 创建日期字符串列表（从今天到未来滚动七天）
'''

now_time = datetime.date.today()
list_date_time, list_date = shareFunction.data_list_create(7, now_time, 7)

# ProducePlan json信息读入
'''
ProducePlan主要数据读入
1. orders: 订单信息
2. inventory: 库存信息
3. arrival: 到货信息
'''

arrive_interval_days = 2  # 全局变量，表示到货子件变成可用的相对日期

with open(ProducePlan, "r", encoding="utf-8") as f_json:
    info = f_json.read()
    data_list = json.loads(info)
    df_orders = pd.DataFrame(data_list["packingPlanRequest"])  # 分装需求
    df_inventory = pd.DataFrame(data_list["stockInfo"])  # 库存信息
    df_arrival = pd.DataFrame(data_list["arrivalInfo"])  # 到货信息
    df_last_produce = pd.DataFrame(data_list["schedulingRequest"])  # 排产需求（多行）

print('df_last_produce:\n {}'.format(df_last_produce))
Lock = ProducePlanParse.lock_data_parse(df_last_produce, now_time)  # 解析锁定的计划
OrderFull, Order = ProducePlanParse.orders_f_data_parse(df_orders, Lock, list_date)  # 解析需求提报计划锁定数据
InventoryInitial = ProducePlanParse.I_0_data_inventory_parse(df_inventory)  # 解析库存数据Inventory
Arr = ProducePlanParse.arr_data_parse(df_arrival, now_time, arrive_interval_days)  # 解析到货信息

# BOM基础数据json信息读入与解析
with open(Bom, "r", encoding="utf-8") as f_json_bom:
    info_bom = f_json_bom.read()
    data_list_bom = json.loads(info_bom)
    df_bom = pd.DataFrame(data_list_bom)
bomsParse.bom_data_check(7, df_bom, OrderFull, ScheduleProductionResult, data_list['requestId'])
BOM = bomsParse.bom_data_parse(df_bom, OrderFull)  # Order -> OrderFull

# calendar基础数据日历(休息日)读入与解析
with open(Calendar, "r", encoding="utf-8") as f_json:
    info = f_json.read()
    data_list_calendar = json.loads(info)
    Calendar_df_1 = pd.DataFrame(data_list_calendar)
    Calendar_df = Calendar_df_1.explode('dayOff')
    Calendar_df.reset_index(drop=True, inplace=True)
print('Calendar_df\n: {}\n Calendar_df.type\n{}'.format(Calendar_df, Calendar_df.shape[0]))

# capacity基础数据分装产能读入与解析
with open(Capacity, "r", encoding="utf-8") as f_json_capacity:
    info_capacity = f_json_capacity.read()
    data_list_capacity = json.loads(info_capacity)
    df_capacity = pd.DataFrame(data_list_capacity)
capacityParse.capacity_data_check(7, OrderFull, df_capacity, ScheduleProductionResult, data_list['requestId'])
PackingCapacity = capacityParse.pc_data_parse(7, df_capacity, Calendar_df, now_time, list_date)

# priority优先级json信息读入与解析
with open(Priority, "r", encoding="utf-8") as f_json_priority:
    info_priority = f_json_priority.read()
    data_list_priority = json.loads(info_priority)
    df_priority = pd.DataFrame(data_list_priority)
Wei = priorityParse.wei_data_parse(df_priority)
add_date = datetime.datetime.strptime(data_list['fixedDate'], "%Y-%m-%d").date()  # 传入的锁定天数为一个日期，可能为可以加单日期或隔天可以加单？
add_day_num = (add_date - now_time).days  # 转化为第n天，今天24，数据中为26号，则为排产的第二天
# Sample
sample_data = model.get_sample(Order, BOM, 7)

#  开始建模过程
# 1）模型参数初始化
LOCK_NUM = data_list['lockDays']  # 锁定三天
PACK_RANGE = 7  # 分装天数
T = range(1, PACK_RANGE + 1)
FIX_NUM = add_day_num
if FIX_NUM < 0:
    FIX_NUM = 0
INVENTORY_SCALE = 10000
# 目标函数
loseSum = 0
delaySum = 0
h_c = 0
# lists
# print('Order:\n{0},\n lock: \n {1}'.format(Order.head(), Lock.head()))

N = list(Order['n'].unique())  # 渠道数量
WAREHOUSE = list(Order['warehouse'].unique())  # 仓库列表
PACKAGE = list(Order['package'].unique())  # 礼盒列表
ORDER_ID = list(OrderFull['id'].unique())  # 全部编号列表
MappingTable = pd.DataFrame()
LOCK_ID = list(Lock['id'].unique())  # 锁定订单编号
SAMPLE = list(sample_data.unique())  # 子件列表
CHANNEL = model.n_2_channel_list(MappingTable, N)

X_INDEX, X1_INDEX = model.get_xindex_x1index(ORDER_ID, OrderFull, T, PACK_RANGE, FIX_NUM)
PackSample = model.get_package_sample(BOM, PACKAGE)
# 分块后的索引
SUB_X_INDEX = model.get_sub_index(X_INDEX, ORDER_ID, WAREHOUSE, T)

# 模型中对于锁定与库存之间的逻辑的检验
ProducePlanParse.Inventory_data_check(InventoryInitial, Lock, PackSample, BOM, data_list['requestId'], ScheduleProductionResult)

data_prepare_end = time.time()  # 数据准备时间结束函数
print('parameters are ready, time cost are: {0}s'.format(str(data_prepare_end - data_prepare_start)))

# 2） 模型变量创建
# 声明求解器(实例化求解器)
solver = pywraplp.Solver.CreateSolver('SCIP')  # 创建一个基于GLOP后端的线性求解器
if not solver:
    print('solver setup failed!!')

INFINITY = solver.infinity()
# 初始化[x]tupledict变量子集
x = model.create_x_tupledict(ORDER_ID, X_INDEX, solver, INFINITY, PACK_RANGE)
# 创建[I]tupledict变量子集
invent = model.create_invent_tupledict(WAREHOUSE, SAMPLE, T, solver, INFINITY)
# 初始化[x_1]tupledict变量子集
x_1 = model.create_x_1_tupledict(ORDER_ID, X1_INDEX, solver, INFINITY, PACK_RANGE)
# 初始化[x_2]tupledict变量子集
x_2 = model.create_x_2_tupledict(ORDER_ID, X_INDEX, solver, INFINITY, PACK_RANGE)

# 3) 设置求解器约束
# 添加x变量的锁定库存的约束
for line in range(Lock.shape[0]):
    i = Lock.loc[line, :]
    if i['o_t'] > 0:
        # print('i: {}'.format(i))
        solver.Add(x[i['id'], i['package'], i['n'], i['warehouse'], i['s_t'], i['o_t'], i['t']] == i['num'])
        # print('x: {}\n {}'.format([i['id'], i['package'], i['n'], i['warehouse'], i['s_t'], i['o_t'], i['t']],
        # i['num']))
# 添加初始库存约束
print('Inventory constraints begin...')
for k, ch, s in itertools.product(WAREHOUSE, CHANNEL, SAMPLE):
    x_sum = 0
    # 循环还有优化的空间
    for i in SUB_X_INDEX[k, 1]:
        if ch == model.n_2_channel(MappingTable, i['k'], i['n']):
            if s in PackSample[i['m']]:
                bom_nums = model.get_bom(BOM, i['m'], s)
                x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['t']] * bom_nums
    solver.Add(x_sum + invent[k, ch, s, 1] == model.get_inventory(InventoryInitial, k, ch, s))

# 添加到锁定期的库存约束
if LOCK_NUM > 0:
    for k, ch, s, t in itertools.product(WAREHOUSE, CHANNEL, SAMPLE, range(2, LOCK_NUM + 1)):
        x_sum = 0
        for i in SUB_X_INDEX[k, t]:
            if ch == model.n_2_channel(MappingTable, i['k'], i['n']):
                if s in PackSample[i['m']]:
                    bom_nums = model.get_bom(BOM, i['m'], s)
                    x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['t']] * bom_nums
        solver.Add(x_sum + invent[k, s, t] == invent[k, s, t - 1])

# 添加到决策末期的库存约束
for k, ch, s, t in itertools.product(WAREHOUSE, CHANNEL, SAMPLE, range(LOCK_NUM + 1, PACK_RANGE + 1)):
    x_sum = 0
    for i in SUB_X_INDEX[k, t]:
        if ch == model.n_2_channel(MappingTable, i['k'], i['n']):
            if s in PackSample[i['m']]:
                bom_nums = model.get_bom(BOM, i['m'], s)
                x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['t']] * bom_nums
    solver.Add(x_sum + invent[k, s, t] == invent[k, s, t - 1] + model.get_arr(Arr, k, s, t))
print('Inventory constraints done...')

# 添加需求约束
for i_d in ORDER_ID:
    if OrderFull[OrderFull['id'] == i_d]['isLock'].values[0] != 1:
        x_sum = 0
        for i in X1_INDEX[i_d]:
            for pt in range(FIX_NUM + 1, i['o_t'] + 1):
                x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], pt]
        solver.Add(x_sum + x_1[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t']] ==
                   model.get_demand(Order, i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], PACK_RANGE))

for i_d in ORDER_ID:
    if OrderFull[OrderFull['id'] == i_d]['isLock'].values[0] != 1:
        for i in X_INDEX[i_d]:
            if i['t'] >= i['o_t']:
                x_sum = 0
                for t_sum in range(FIX_NUM + 1, i['t'] + 1):
                    x_sum = x_sum + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], t_sum]
                solver.Add(x_sum + x_2[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['t']] ==
                           model.get_demand(Order, i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], PACK_RANGE))
print('Demond done...')


# 添加产能约束
for k, t in itertools.product(WAREHOUSE, T):
    warehouse_list = list(PackingCapacity[PackingCapacity['pack_factory'] == k]['warehouse'].unique())
    rate = 0
    for i_d in ORDER_ID:
        for i in X_INDEX[i_d]:
            if i['k'] in warehouse_list and i['t'] == t:
                rate = rate + x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['t']] / \
                       model.warehouse_s(PackingCapacity, k, i['m'], t)
    solver.Add(rate <= float(model.warehouse_c(PackingCapacity, k, t)))
print('Capacity done...')


# 4) 设置目标函数

for i_d in ORDER_ID:
    for i in X1_INDEX[i_d]:
        delaySum = delaySum + model.objective_weight(i['m'], i['n'], i['k'], i['s_t'], i['o_t'], Wei, PACK_RANGE, 1) * \
                   x_1[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t']]

for i_d in ORDER_ID:
    for i in X_INDEX[i_d]:
        if i['t'] > i['o_t']:
            loseSum = loseSum + model.objective_weight(i['m'], i['n'], i['k'], i['s_t'], i['o_t'], Wei, PACK_RANGE, 1) * \
                      x_2[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['t']]

for k, ch, s, t in itertools.product(WAREHOUSE, CHANNEL, SAMPLE, T):
    h_c = h_c + invent[k, ch, s, t]

solver.Minimize(delaySum + loseSum + h_c / INVENTORY_SCALE)
print('Objective function setting done ！')

# 5）开始训练
parameters = pywraplp.MPSolverParameters()
parameters.SetDoubleParam(pywraplp.MPSolverParameters.RELATIVE_MIP_GAP,  1e-8)
print('Optimizing...')
result_status = solver.Solve(parameters)
if result_status == solver.OPTIMAL:
    assert solver.VerifySolution(1e-7, True)
    print('Number of variables =', solver.NumVariables())
    print('Number of constraints =', solver.NumConstraints())
    # The objective value of the solution.
    print('Optimal objective value = %d' % solver.Objective().Value())
    print('\nAdvanced usage:')
    print('Problem solved in %f milliseconds' % solver.wall_time())
    print('Problem solved in %d iterations' % solver.iterations())
    # 6）开始写结果
    # 创建Json格式文件， 结果写入data当中
    request_id = data_list['requestId']
    res = {'code': 200, 'msg': "success", 'requestId': request_id, 'data': []}
    # TODO (杨江南)： 这个输出文件需要优化
    for i in range(Lock.shape[0]):
        for od in range(df_last_produce.shape[0]):
            if Lock.loc[i, 'id'] == str(df_last_produce.loc[od, 'scheduleId']):
                last_res = {'lockDays': LOCK_NUM,
                            'packingPlanId': int(df_last_produce.loc[od, 'packingPlanId']),
                            'packingPlanSerialNum': int(df_last_produce.loc[od, 'packingPlanSerialNum']),
                            'demandCommitDate': df_last_produce.loc[od, 'demandCommitDate'],
                            'scheduleId': int(df_last_produce.loc[od, 'scheduleId']),
                            'scheduleVersion': int(df_last_produce.loc[od, 'scheduleVersion']),
                            'warehouse': str(df_last_produce.loc[od, 'warehouse']),
                            'subChannel': str(df_last_produce.loc[od, 'subChannel']),
                            'productCode': str(df_last_produce.loc[od, 'productCode']),
                            'productName': df_last_produce.loc[od, 'productName'],
                            'bomVersion': str(df_last_produce.loc[od, 'bomVersion']),
                            'scheduleDate': df_last_produce.loc[od, 'scheduleDate'],
                            'planQuantity': int(df_last_produce.loc[od, 'planQuantity']),
                            'batchRequire': str(df_last_produce.loc[od, 'batchRequire']),
                            'deliveryFactory': str(df_last_produce.loc[od, 'deliveryFactory'])}
                res['data'].append(last_res)
    for i_d in ORDER_ID:
        for i in X_INDEX[i_d]:
            if x[i['id'], i['m'], i['n'], i['k'], i['s_t'], i['o_t'], i['t']].solution_value() > 0:
                print((i['id'], i['m'], i['n'], i['k'], i['s_t'],
                       i['o_t'], i['t']), x[i['id'], i['m'], i['n'],
                                            i['k'], i['s_t'], i['o_t'], i['t']].solution_value())
                for od in range(Order.shape[0]):
                    if i['id'] == Order.loc[od, 'id']:
                        mid_res = {
                            'lockDays': LOCK_NUM,
                            'packingPlanId': int(df_orders.loc[od, 'packingPlanId']),
                            'packingPlanSerialNum': int(df_orders.loc[od, 'packingPlanSerialNum']),
                            'demandCommitDate': df_orders.loc[od, 'demandCommitDate'],
                            'scheduleId': None,
                            'scheduleVersion': None,
                            'warehouse': str(df_orders.loc[od, 'warehouse']),
                            'subChannel': str(df_orders.loc[od, 'subChannel']),
                            'productCode': str(df_orders.loc[od, 'giftBoxCode']),
                            'productName': df_orders.loc[od, 'giftBoxName'],
                            'bomVersion': str(df_orders.loc[od, 'bomVersion']),
                            'scheduleDate': list_date[i['t'] - 1],
                            'planQuantity': int(x[i['id'], i['m'], i['n'],
                                                  i['k'], i['s_t'], i['o_t'], i['t']].solution_value()),
                            'batchRequire': str(df_orders.loc[od, 'ageRequire']),
                            'deliveryFactory': None
                        }
                        res['data'].append(mid_res)
    # 保存Json文件到7days_schedule_plan_result.json当中
    with open(ScheduleProductionResult, 'w', encoding='utf-8') as write_f:
        write_f.write(json.dumps(res, indent=4, ensure_ascii=False))
    print('the result is be saved!')
    print('Objective Value:{}'.format(solver.Objective().Value()))
    sys.stdout = save_stdout
    print(True)
elif (result_status == solver.FEASIBLE):
    print('A potentially suboptimal solution was found.')  # 发现了一个潜在的次优解决方案
elif (result_status == solver.INFEASIBLE):
    print("Problem is infeasible")
elif (result_status == solver.UNBOUNDED):
    print("Problem is unbounded")
elif (result_status == solver.ABNORMAL):
    print("Problem is abnormal")
elif (result_status == solver.NOT_SOLVED):
    print("Problem is not solved")




