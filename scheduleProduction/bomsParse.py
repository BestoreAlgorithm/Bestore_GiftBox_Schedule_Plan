# -*- coding: utf-8 -*-
# @Time    : 2022-08-19 17:56
# @Author  : zhoujian.Pray
# @FileName: bomsParse.py
# @Software: PyCharm
import pandas as pd


def bom_data_parse(df_bom, data_orders):
    '''
    获取分装计划中出现的礼盒(成品编码+bom版本号)Bom数据
    :param df_bom: DataFrame, 原Bom单基础数据
    :param data_orders: DataFrame, 清洗后的分装计划需求数据
    :return: DataFrame, 分装计划中出现的礼盒Bom数据
            返回的Bom数据仅包含成品编码、子件编码、子件数量三列
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


def bom_data_parse_week(df_bom):
    '''
    已舍弃, 确认未在其他地方使用后可删除
    '''
    df_bom["bomVersion"] = df_bom["bomVersion"].astype(str)  # 将bomVersion列转化为字符串
    df_bom["productCode"] = df_bom["productCode"].astype(str)  # 将productCode列转化为字符串
    df_bom["subCode"] = df_bom["subCode"].astype(str)  # 将subCode列转化为字符串
    df_bom["subQuantity"] = df_bom["subQuantity"].astype(str)  # 将subQuantity列转化为字符串
    data_bom = pd.DataFrame(df_bom, columns=['productCode', 'subCode', 'subQuantity', 'bomVersion'])
    data_bom.rename(columns={'productCode': 'pack', 'subCode': 'sample', 'subQuantity': 'num',
                             'bomVersion': 'version'}, inplace=True)
    BOM = pd.DataFrame(data_bom)
    return BOM
