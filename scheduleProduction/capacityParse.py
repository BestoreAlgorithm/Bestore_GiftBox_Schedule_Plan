# -*- coding: utf-8 -*-
# @Time    : 2022-08-19 17:57
# @Author  : zhoujian.Pray
# @FileName: capacityParse.py
# @Software: PyCharm

import pandas as pd
import datetime
import json
import sys
save_stdout = sys.stdout  # 保存当前控制台的输出路径


def pc_data_parse(category, df_capacity, Calendar_df, now_time, list_date):
    '''
    产能数据清洗函数, 会对产能按仓库和礼盒进行汇总。将工时进行归一处理, 并将工时数值转化到(乘数)礼盒生产速率上。
    :param category: int, 生成所需产能数据帧的标识符
            当 category == 7: 生成7天排产计划的产能数据帧
            当 category == 13: 生成13周分装计划的产能数据帧
    :param df_capacity: DataFrame, 原产能基础数据
    :param Calendar_df: DataFrame, 工厂日历基础数据
    :param now_time: datetime, 当前时间的日期
    :param list_date: list['str'],  需要进行排产计划或分装计划的周次(周末)日期列表, 根据category的值不同而不同
    :return: DataFrame, 不同算法所需的经过汇总处理的产能数据帧:
                仓库编码
                成品编码
                产线编码
                生产速率(汇总)
                产线工时(归一)
                相对时间节点(t:单日或某一整周)
    Tip: 生成的产能数据帧中，生产速率是汇总后的产速, 等于原产速乘上工时, 而工时归为1
    '''
    df_capacity['productCode'] = df_capacity['productCode'].astype(str)

    if category == 7:
        Calendar_df.rename(columns={'dayOff': 't', 'packingFactoryCode': 'pack_factory'}, inplace=True)
        pc = pd.DataFrame(columns=['pack_factory', 'warehouse', 'line', 'package', 'num', 'hours', 't'])  # 最终取出的DataFrame结果
        df_capacity_explode = df_capacity.explode('warehouse')
        df_capacity_explode.reset_index(drop=True, inplace=True)
        data_capacity = pd.DataFrame(columns=['pack_factory', 'warehouse', 'line', 'package', 'num',
                                              'hours', 'startTime', 'endTime'])
        for i in range(df_capacity_explode.shape[0]):
            line_start_datetime = datetime.datetime.strptime(df_capacity_explode.loc[i, 'startDate'], "%Y-%m-%d").date()
            line_end_datetime = datetime.datetime.strptime(df_capacity_explode.loc[i, 'endDate'], "%Y-%m-%d").date()
            line_start_t = (line_start_datetime - now_time).days  # 为了方便比较，先统一减去now_time，转化为数值形式的相对时间
            line_ent_t = (line_end_datetime - now_time).days
            data_capacity = pd.concat([data_capacity, pd.DataFrame(
                {'pack_factory': df_capacity_explode.loc[i, 'factoryCode'],
                 'warehouse': df_capacity_explode.loc[i, 'warehouse'],
                 'line': df_capacity_explode.loc[i, 'lineNo'],
                 'package': df_capacity_explode.loc[i, 'productCode'],
                 'num': df_capacity_explode.loc[i, 'repackagingAbility'],
                 'hours': df_capacity_explode.loc[i, 'manHour'],
                 'startTime': line_start_t,
                 'endTime': line_ent_t}, index=[0])], ignore_index=True)
        # 对data_capacity和list_date进行Merge操作：data_capacity 和 Calendar_df 各加一个辅助列sup
        data_capacity['sup'] = 1
        df_date = pd.DataFrame(list_date, columns=['t'])
        df_date['sup'] = 1
        Calendar_df['sup'] = 1
        Capacity_date_df = pd.merge(data_capacity, df_date, how='outer', on='sup')
        Capacity_date_df.drop(['sup'], axis=1, inplace=True)
        # print('Capacity_date_df：\n{}'.format(Capacity_date_df.head(20)))
        Capacity_date_Calendar = pd.merge(Capacity_date_df, Calendar_df, how='left', on=['pack_factory', 't'])
        # print('Capacity_date_Calendar：\n{}'.format(Capacity_date_Calendar.head(20)))
        Capacity_date_Calendar.loc[Capacity_date_Calendar[Capacity_date_Calendar.sup == 1].index.tolist(), 'hours'] = 0
        Capacity_date_Calendar.drop(['sup'], axis=1, inplace=True)
        # 产线有效期判断，并转化datetime -> int date
        for i in range(Capacity_date_Calendar.shape[0]):
            date_in_Capacity = datetime.datetime.strptime(Capacity_date_Calendar.loc[i, 't'], "%Y-%m-%d").date()
            date_to_t = (date_in_Capacity - now_time).days  # 将datetime形式的日期转化为数值型
            if date_to_t < Capacity_date_Calendar.loc[i, 'startTime'] or date_to_t > Capacity_date_Calendar.loc[i, 'endTime']:
                Capacity_date_Calendar.loc[i, 'hours'] = 0
            Capacity_date_Calendar.loc[i, 't'] = date_to_t
        # 工时归一处理，用于产能获取函数中识别为已汇总
        for i in range(Capacity_date_Calendar.shape[0]):
            deal_hour = 1
            if Capacity_date_Calendar.loc[i, 'hours'] == 0:
                deal_num = 0
            else:
                conversion_multiple = Capacity_date_Calendar.loc[i, 'hours']  # 获取原工时，即工时归一后，需要给产速乘的倍数
                the_pack_ratio = Capacity_date_Calendar.loc[i, 'num']  # 获取产速
                deal_num = the_pack_ratio * conversion_multiple  # 新的分装产能
            pc = pd.concat([pc, pd.DataFrame({'pack_factory': Capacity_date_Calendar.loc[i, 'pack_factory'],
                                              'warehouse': Capacity_date_Calendar.loc[i, 'warehouse'],
                                              'line': Capacity_date_Calendar.loc[i, 'line'],
                                              'package': Capacity_date_Calendar.loc[i, 'package'],
                                              'num': deal_num,
                                              'hours': deal_hour,
                                              't': Capacity_date_Calendar.loc[i, 't']}, index=[0])], ignore_index=True)
        print('pc：\n{}'.format(pc.head(20)))

    elif category == 13:
        Calendar_df.rename(columns={'dayOff': 't', 'packingFactoryCode': 'warehouse'}, inplace=True)
        data_capacity = pd.DataFrame(columns=['warehouse', 'line', 'package', 'num', 'hours',
                                              'startTime', 'endTime'])
        pc = pd.DataFrame(columns=['warehouse', 'line', 'package', 'num', 'hours', 't'])
        for i in range(df_capacity.shape[0]):
            line_start_datetime = datetime.datetime.strptime(df_capacity.loc[i, 'startDate'], "%Y-%m-%d").date()
            line_end_datetime = datetime.datetime.strptime(df_capacity.loc[i, 'endDate'], "%Y-%m-%d").date()
            line_start_t = (line_start_datetime - now_time).days  # 为了方便比较，先统一减去now_time，转化为数值形式的相对时间
            line_ent_t = (line_end_datetime - now_time).days
            data_capacity = pd.concat([data_capacity, pd.DataFrame({'warehouse': df_capacity.loc[i, 'warehouse'],
                                                                    'line': df_capacity.loc[i, 'lineNo'],
                                                                    'package': df_capacity.loc[i, 'productCode'],
                                                                    'num': df_capacity.loc[i, 'repackagingAbility'],
                                                                    'hours': df_capacity.loc[i, 'manHour'],
                                                                    'startTime': line_start_t,
                                                                    'endTime': line_ent_t},
                                                                   index=[0])], ignore_index=True)
        the_first_week_date = datetime.datetime.strptime(list_date[0], "%Y-%m-%d").date()
        week_before_now = the_first_week_date + datetime.timedelta(days=-7)  # 第一周的前一周日期(特殊情况)
        week_before_str = week_before_now.strftime('%Y-%m-%d')  # str形式的日期
        total_list_date = list_date.copy()  # 新生成一个附带前一周的列表
        total_list_date.insert(0, week_before_str)
        # data_capacity 和 Calendar_df 各加一个辅助列sup
        data_capacity['sup'] = 1
        df_date = pd.DataFrame(total_list_date, columns=['t'])
        df_date['sup'] = 1
        Capacity_date_df = pd.merge(data_capacity, df_date, how='outer', on='sup')
        Capacity_date_df.drop(['sup'], axis=1, inplace=True)
        # 识别产能产线的有效日期，并用集合运算解决休息日与有效日期
        for i in range(Capacity_date_df.shape[0]):
            the_hour = Capacity_date_df.loc[i, 'hours']
            # 日期识别，将日期转化成基于当前日期的相对天数
            the_week_end_date = datetime.datetime.strptime(Capacity_date_df.loc[i, 't'], "%Y-%m-%d").date()
            the_week_end_t = (the_week_end_date - now_time).days  # 周末相对日期
            the_week_start_t = the_week_end_t - 6  # 周一相对日期
            line_start_time = Capacity_date_df.loc[i, 'startTime']
            line_end_time = Capacity_date_df.loc[i, 'endTime']
            if the_week_start_t <= line_start_time and the_week_end_t <= line_end_time:  # 交集情况1：产线开始时间晚于周开始，产线失效时间晚于周结束
                set_effective_date = set(list(range(line_start_time, the_week_end_t + 1)))
            elif the_week_start_t >= line_start_time and the_week_end_t >= line_end_time:
                set_effective_date = set(list(range(the_week_start_t, line_end_time + 1)))
            elif the_week_start_t <= line_start_time and the_week_end_t >= line_end_time:
                set_effective_date = set(list(range(line_start_time, line_end_time + 1)))
            elif the_week_start_t >= line_start_time and the_week_end_t <= line_end_time:
                set_effective_date = set(list(range(the_week_start_t, the_week_end_t + 1)))
            else:
                set_effective_date = {}
            list_rest_day = []
            for j in range(Calendar_df.shape[0]):
                if Calendar_df.loc[j, 'warehouse'] == Capacity_date_df.loc[i, 'warehouse']:
                    rest_date = datetime.datetime.strptime(Calendar_df.loc[j, 't'], "%Y-%m-%d").date()
                    rest_date_num = (rest_date - now_time).days
                    list_rest_day.append(rest_date_num)
            set_rest_day = set(list_rest_day)
            diff_res = set_effective_date - set_rest_day
            week_hours = the_hour * len(diff_res)
            Capacity_date_df.loc[i, 'hours'] = week_hours
        # 对产能进行归一汇总处理
        for i in range(Capacity_date_df.shape[0]):
            deal_hour = 1
            if Capacity_date_df.loc[i, 'hours'] == 0:
                deal_num = 0
            else:
                conversion_multiple = Capacity_date_df.loc[i, 'hours']  # 获取原工时，即工时归一后，需要给产速乘的倍数
                the_pack_ratio = Capacity_date_df.loc[i, 'num']  # 获取产速
                deal_num = the_pack_ratio * conversion_multiple  # 新的分装产能
            for j in range(len(total_list_date)):
                if Capacity_date_df.loc[i, 't'] == total_list_date[j]:
                    the_t = j
                    if j == 0:
                        the_t = the_t - 1
            pc = pd.concat([pc, pd.DataFrame({'warehouse': Capacity_date_df.loc[i, 'warehouse'],
                                              'line': Capacity_date_df.loc[i, 'line'],
                                              'package': Capacity_date_df.loc[i, 'package'],
                                              'num': deal_num,
                                              'hours': deal_hour,
                                              't': the_t}, index=[0])], ignore_index=True)
        # 增加14周的无限产能
        for i in range(data_capacity.shape[0]):
            pc = pd.concat([pc, pd.DataFrame({'warehouse': str(data_capacity.loc[i, 'warehouse']),
                                              'line': str(data_capacity.loc[i, 'line']),
                                              'package': str(data_capacity.loc[i, 'package']),
                                              'num': 90000000,
                                              'hours': 1,
                                              't': 14}, index=[0])], ignore_index=True)
    print('(capacity)产能数据\n{0}'.format(pc.head()))
    return pc


def capacity_data_check(category, orders, df_capacity_initial, ScheduleProductionResult, request_id):
    '''
    检查产能是否与需求数据配套, 即排产、分装计划中出现的仓库和成品编码组合, 是否在产能基础数据中已经存在。
    :param category: int, 所执行算法的标识:
                当 category == 7: 返回检查信息到7天排产计划算法的接口中
                当 category == 13: 返回检查信息到13周分装计划算法的接口中
    :param orders: DataFrame, 清洗后的需求数据(分装计划数据或7天排产计划中的锁定+需求数据)
    :param df_capacity: DataFrame, 原产能基础数据
    :param ScheduleProductionResult: 合法的结果json文件存放的路径
    :param request_id: 每次运行的主数据requestId
    :return:无返回值, 数据配套则继续，否则终止
    Tip: 要保证结果json文件的路径的正确性，根据输入的category不同而不同
    '''
    if category == 7:
        df_capacity = df_capacity_initial.explode('warehouse')
        df_capacity.reset_index(drop=True, inplace=True)
    else:
        df_capacity = df_capacity_initial
    for i in range(orders.shape[0]):
        check_flag = 0
        for j in range(df_capacity.shape[0]):
            if orders.loc[i, 'warehouse'] == df_capacity.loc[j, 'warehouse'] and orders.loc[i, 'package'] == \
                    df_capacity.loc[j, 'productCode']:
                check_flag = 1
                break
        if check_flag == 0:
            print("产能数据中没有礼盒{0}在库存工厂{1}中的产能信息".format(orders.loc[i, 'package'], orders.loc[i, 'warehouse']))
            if category == 7:
                res = {'code': 701,
                       'msg': "产能基础数据中没有礼盒{0}在库存工厂{1}中的产能信息".format(orders.loc[i, 'package'], orders.loc[
                                                                                          i, 'warehouse']),
                       'requestId': request_id, 'data': []}
            elif category == 13:
                res = {'code': 701,
                       'msg': "产能基础数据中没有礼盒{0}在库存工厂{1}中的产能信息".format(orders.loc[i, 'package'], orders.loc[
                                                                                          i, 'warehouse']),
                       'data': {'packingPlanInfo': [], 'subSupplyPlanInfo': [], 'requestId': request_id}}
            with open(ScheduleProductionResult, 'w', encoding='utf-8') as write_f:
                write_f.write(json.dumps(res, indent=4, ensure_ascii=False))
            sys.stdout = save_stdout
            print(True)
            sys.exit(0)
