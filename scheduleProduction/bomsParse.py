# -*- coding: utf-8 -*-
# @Time    : 2022-08-19 17:56
# @Author  : zhoujian.Pray
# @FileName: bomsParse.py
# @Software: PyCharm
import pandas as pd
import json
import sys
save_stdout = sys.stdout  # 保存当前控制台的输出路径


def bom_data_parse(df_bom, data_orders):
    '''
    获取分装计划中出现的礼盒(成品编码+bom版本号)Bom数据
    :param df_bom: DataFrame, 原Bom单基础数据
    :param data_orders: DataFrame, 清洗后的分装计划需求数据
    :return: DataFrame, 分装计划中出现的礼盒Bom数据:
                成品编码
                物料编码
                物料数量
    Tip:1.两个算法共用一个bom获取函数，只需要改变入参的data_orders, 对于7天排产计划算法，需要输入加上锁定后的总订单需求数据帧
        2.分装计划中同一成品编码不会出现多个bom版本号的情况，因此返回的数据帧中舍去了bom版本号一列
    '''
    df_bom["bomVersion"] = df_bom["bomVersion"].astype(str)  # 将bomVersion列转化为字符串
    df_bom["productCode"] = df_bom["productCode"].astype(str)  # 将productCode列转化为字符串
    df_bom["subCode"] = df_bom["subCode"].astype(str)  # 将subCode列转化为字符串
    df_bom["subQuantity"] = df_bom["subQuantity"].astype(str)  # 将subQuantity列转化为字符串
    # 从ProducePlan的json文件中解析出的需求提报计划data_orders中拿出package和bom并去重
    bom_in_order = pd.DataFrame(data_orders, columns=['package', 'bom'])
    bom_in_order.drop_duplicates(subset=['package', 'bom'], keep='first', inplace=True)
    bom_in_order.reset_index(drop=True, inplace=True)
    bom_in_order.rename(columns={'package': 'productCode', 'bom': 'bomVersion'}, inplace=True)
    df_merge = pd.merge(bom_in_order, df_bom, on=['productCode', 'bomVersion'])  # 按照物料编码和版本号合并inner
    BOM = pd.DataFrame(df_merge, columns=['productCode', 'subCode', 'subQuantity'])
    BOM.rename(columns={'productCode': 'pack', 'subCode': 'sample', 'subQuantity': 'num'}, inplace=True)
    BOM["num"] = BOM["num"].astype(float)  # add
    print('(BOM)匹配到需求计划订单的BOM数据:{0}'.format(BOM.head()))
    return BOM


def bom_data_check(category, df_bom, data_orders, ScheduleProductionResult, request_id):
    '''
    检查BOM数据是否与需求数据配套, 即排产、分装计划中出现的成品编码和bom版本号组合, 是否在BOM基础数据中已经存在。
    :param category: int, 所执行算法的标识:
                当 category == 7: 返回检查信息到7天排产计划算法的接口中
                当 category == 13: 返回检查信息到13周分装计划算法的接口中
    :param df_bom: DataFrame, 原BOM单基础数据
    :param data_orders: DataFrame, 清洗后的需求数据(分装计划数据或7天排产计划中的锁定+需求数据)
    :param ScheduleProductionResult: 合法的结果json文件存放的路径
    :param request_id: 每次运行的主数据requestId
    :return: 无返回值, 数据配套则继续，否则终止
    Tip: 要保证结果json文件的路径的正确性，根据输入的category不同而不同
    '''
    bom_in_order = pd.DataFrame(data_orders, columns=['package', 'bom'])
    bom_in_order.drop_duplicates(subset=['package', 'bom'], keep='first', inplace=True)
    bom_in_order.reset_index(drop=True, inplace=True)
    for i in range(bom_in_order.shape[0]):
        check_flag = 0
        for j in range(df_bom.shape[0]):
            if bom_in_order.loc[i, 'package'] == df_bom.loc[j, 'productCode'] and bom_in_order.loc[i, 'bom'] \
                    == df_bom.loc[j, 'bomVersion']:
                check_flag = 1
                break
        if check_flag == 0:
            print("Bom数据中没有礼盒{0}-bom版本号{1}的bom单信息".format(bom_in_order.loc[i, 'package'], bom_in_order.loc[i, 'bom']))
            if category == 7:
                res = {'code': 702,
                       'msg': "Bom基础数据中没有礼盒{0}-bom版本号{1}中的bom单信息".format(
                           bom_in_order.loc[i, 'package'], bom_in_order.loc[i, 'bom']),
                       'requestId': request_id, 'data': []}
            elif category == 13:
                res = {'code': 702,
                       'msg': "Bom基础数据中没有礼盒{0}-bom版本号{1}中的bom单信息".format(
                           bom_in_order.loc[i, 'package'], bom_in_order.loc[i, 'bom']),
                       'data': {'packingPlanInfo': [], 'subSupplyPlanInfo': [], 'requestId': request_id}}
            with open(ScheduleProductionResult, 'w', encoding='utf-8') as write_f:
                write_f.write(json.dumps(res, indent=4, ensure_ascii=False))
            sys.stdout = save_stdout
            print(True)
            sys.exit(0)
