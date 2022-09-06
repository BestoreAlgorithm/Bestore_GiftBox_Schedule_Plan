# -*- coding: utf-8 -*-
# @Time    : 2022-08-30 15:09
# @Author  : zhou_jian.Pray
# @FileName: Json2excel.py
# @Software: PyCharm
import pandas as pd
import json
import os
from scheduleProduction import PackPlanParse

# 设置数据路径全局变量
project_path = os.getcwd()  # 当前路径
jsons_filename = 'jsons'  # 存放数据的文件夹的名称
result_filename = 'result'
result_data_path = project_path + '\\' + result_filename
jsons_data_path = project_path + '\\' + jsons_filename  # 原始数据的路径
ProducePlan = jsons_data_path + '\\' + 'ProducePlan.json'
Bom = jsons_data_path + '\\' + 'Bom.json'
Capacity = jsons_data_path + '\\' + 'Capacity.json'
Priority = jsons_data_path + '\\' + 'Priority.json'
Calendar = jsons_data_path + '\\' + 'Calendar.json'
PackPlan = jsons_data_path + '\\' + 'PackPlan.json'
seven_days_result = result_data_path + '\\' + '7days_schedule_plan_result.json'  # 7天的排产结果
fourteen_weeks_result = result_data_path + '\\' + '13weeks_schedule_plan_result.json'  # 13周的排产结果
print('json数据所在的文件夹路径：', jsons_data_path)
print('排产计划数据所在的文件夹路径：', ProducePlan)
print('Bom主数据所在的文件夹路径：', Bom)
print('产能主数据所在的文件夹路径：', Capacity)
print('优先级主数据所在的文件夹路径：', Priority)
print('日历主数据所在的文件路径：', Calendar)

# ProducePlan.json解析
with open(ProducePlan, "r", encoding="utf-8") as f_json:
    info = f_json.read()
    data_list = json.loads(info)
    df_orders = pd.DataFrame(data_list["packingPlanRequest"])  # 分装需求
    df_inventory = pd.DataFrame(data_list["stockInfo"])  # 库存信息
    df_arrival = pd.DataFrame(data_list["arrivalInfo"])  # 到货信息
    df_last_produce = pd.DataFrame(data_list["schedulingRequest"])  # 排产需求（多行）


# boms.json解析
with open(Bom, "r", encoding="utf-8") as f_json_bom:
    info_bom = f_json_bom.read()
    data_list_bom = json.loads(info_bom)
    df_bom = pd.DataFrame(data_list_bom)

# capacity.json解析
with open(Capacity, "r", encoding="utf-8") as f_json_capacity:
    info_capacity = f_json_capacity.read()
    data_list_capacity = json.loads(info_capacity)
    df_capacity = pd.DataFrame(data_list_capacity)

# Priority.json 数据解析
with open(Priority, "r", encoding="utf-8") as f_json_priority:
    info_priority = f_json_priority.read()
    data_list_priority = json.loads(info_priority)
    df_priority = pd.DataFrame(data_list_priority)

# 解析PackPlan.json数据
with open(PackPlan, "r", encoding="utf-8") as f_json:
    info = f_json.read()
    data_list = json.loads(info)
df_orders, df_samples = PackPlanParse.orders_data_parse(data_list)

# 解析7days_schedule_plan_result.json数据
with open(seven_days_result, "r", encoding="utf-8") as f_json:
    info = f_json.read()
    data_list = json.loads(info)
    result_7days = pd.DataFrame(data_list["data"])  # 7天分装结果

# 解析13weeks_schedule_plan_result.json数据
with open(fourteen_weeks_result, "r", encoding="utf-8") as f_json:
    info = f_json.read()
    data_list = json.loads(info)
    result_13weeks_packingPlanInfo = pd.DataFrame(data_list["data"]["packingPlanInfo"])  # 13周分装结果
    result_13weeks_subSupplyPlanInfo = pd.DataFrame(data_list["data"]["subSupplyPlanInfo"])  # 13周分装结果


with pd.ExcelWriter('data.xlsx') as write:
    df_orders.to_excel(write, sheet_name='df_orders', index=False)
    df_inventory.to_excel(write, sheet_name='df_inventory', index=False)
    df_arrival.to_excel(write, sheet_name='df_arrival', index=False)
    df_last_produce.to_excel(write, sheet_name='df_last_produce', index=False)
    df_bom.to_excel(write, sheet_name='df_bom', index=False)
    df_capacity.to_excel(write, sheet_name='df_capacity', index=False)
    df_priority.to_excel(write, sheet_name='df_priority', index=False)
    df_orders.to_excel(write, sheet_name='df_orders', index=False)
    df_samples.to_excel(write, sheet_name='df_samples', index=False)
    result_7days.to_excel(write, sheet_name='result_7days', index=False)
    result_13weeks_packingPlanInfo.to_excel(write, sheet_name='result_13weeks_PPI', index=False)
    result_13weeks_subSupplyPlanInfo.to_excel(write, sheet_name='result_13weeks_SPI', index=False)


