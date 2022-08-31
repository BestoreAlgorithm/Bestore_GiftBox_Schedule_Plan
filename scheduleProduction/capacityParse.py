# -*- coding: utf-8 -*-
# @Time    : 2022-08-19 17:57
# @Author  : zhoujian.Pray
# @FileName: capacityParse.py
# @Software: PyCharm

import pandas as pd
import datetime


def pc_data_parse(category, df_capacity, calendar_list, list_date):
    df_capacity['productCode'] = df_capacity['productCode'].astype(str)
    data_capacity = pd.DataFrame(df_capacity,
                                 columns=['warehouse', 'lineNo', 'productCode', 'repackagingAbility', 'manHour'])
    data_capacity.rename(columns={'warehouse': 'warehouse',
                                  'lineNo': 'line',
                                  'productCode': 'package',
                                  'repackagingAbility': 'num',
                                  'manHour': 'hours'}, inplace=True)
    # TODO（新增标记）
    pc = pd.DataFrame(columns=['warehouse', 'line', 'package', 'num', 'hours', 't'])
    if category == 7:
        for i in range(data_capacity.shape[0]):
            for j in range(len(list_date)):
                the_warehouse = data_capacity.loc[i, 'warehouse']
                real_hour = data_capacity.loc[i, 'hours']
                the_date = list_date[j]
                check_off_flag = 0
                for k in range(len(calendar_list)):
                    if calendar_list[k]['packingFactoryCode'] == the_warehouse:
                        for d in range(len(calendar_list[k]['dayOff'])):
                            if calendar_list[k]['dayOff'][d] == the_date:
                                check_off_flag = 1
                                break
                if check_off_flag == 1:
                    real_hour = 0
                pc = pd.concat([pc, pd.DataFrame(
                    {'warehouse': str(data_capacity.loc[i, 'warehouse']), 'line': str(data_capacity.loc[i, 'line']),
                     'package': str(data_capacity.loc[i, 'package']), 'num': int(data_capacity.loc[i, 'num']),
                     'hours': real_hour, 't': (j + 1)}, index=[0])], ignore_index=True)
    elif category == 13:
        the_first_week_date = datetime.datetime.strptime(list_date[0], "%Y-%m-%d").date()
        days_before_date = the_first_week_date + datetime.timedelta(days=-7)  # 第一周的前一周日期(特殊情况) # TODO(新增标记)
        days_before = days_before_date.strftime('%Y-%m-%d')  # str形式的日期
        total_list_date = list_date.copy()  # 新生成一个附带前一周的列表
        total_list_date.insert(0, days_before)
        for i in range(data_capacity.shape[0]):
            for j in range(len(total_list_date)):
                the_warehouse = data_capacity.loc[i, 'warehouse']
                day_hour = data_capacity.loc[i, 'hours']
                the_week = total_list_date[j]
                rest_day_num = 0
                for k in range(len(calendar_list)):
                    if calendar_list[k]['packingFactoryCode'] == the_warehouse:
                        for d in range(len(calendar_list[k]['dayOff'])):
                            rest_date = datetime.datetime.strptime(calendar_list[k]['dayOff'][d], "%Y-%m-%d").date()
                            the_week_date = datetime.datetime.strptime(the_week, "%Y-%m-%d").date()
                            dis_day = (the_week_date - rest_date).days
                            if dis_day <= 6 and dis_day >= 0:
                                rest_day_num = rest_day_num + 1
                week_hour = (7 - rest_day_num) * day_hour
                the_t = j  # append in ** 等于't'
                if j == 0:
                    the_t = the_t - 1  # 将前一周由0转换为-1
                pc = pd.concat([pc, pd.DataFrame(
                    {'warehouse': str(data_capacity.loc[i, 'warehouse']), 'line': str(data_capacity.loc[i, 'line']),
                     'package': str(data_capacity.loc[i, 'package']), 'num': int(data_capacity.loc[i, 'num']),
                     'hours': week_hour, 't': int(the_t)}, index=[0])], ignore_index=True)
            pc = pd.concat([pc, pd.DataFrame({'warehouse': str(data_capacity.loc[i, 'warehouse']),
                                              'line':str(data_capacity.loc[i, 'line']),
                                              'package':str(data_capacity.loc[i, 'package']),
                                              'num':int(data_capacity.loc[i, 'num']), 'hours':10000,
                                              't':14}, index=[0])], ignore_index=True)
    print('(capacity)产能数据{0}'.format(pc.head()))
    return pc
