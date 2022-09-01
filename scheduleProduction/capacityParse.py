# -*- coding: utf-8 -*-
# @Time    : 2022-08-19 17:57
# @Author  : zhoujian.Pray
# @FileName: capacityParse.py
# @Software: PyCharm

import pandas as pd
import datetime


def pc_data_parse(category, df_capacity, Calendar_df, list_date):
    df_capacity['productCode'] = df_capacity['productCode'].astype(str)
    data_capacity = pd.DataFrame(df_capacity,
                                 columns=['warehouse', 'lineNo', 'productCode', 'repackagingAbility', 'manHour'])
    data_capacity.rename(columns={'warehouse': 'warehouse',
                                  'lineNo': 'line',
                                  'productCode': 'package',
                                  'repackagingAbility': 'num',
                                  'manHour': 'hours'}, inplace=True)
    pc = pd.DataFrame(columns=['warehouse', 'line', 'package', 'num', 'hours', 't'])
    Calendar_df.rename(columns={'dayOff': 't', 'packingFactoryCode': 'warehouse'}, inplace=True)

    if category == 7:
        now_time = datetime.date.today()
        # data_capacity 和 Calendar_df 各加一个辅助列sup
        data_capacity['sup'] = 1
        df_date = pd.DataFrame(list_date, columns=['t'])
        df_date['sup'] = 1
        Calendar_df['sup'] = 1
        Capacity_date_df = pd.merge(data_capacity, df_date, how='outer', on='sup')
        Capacity_date_df.drop(['sup'], axis=1, inplace=True)
        print('Capacity_date_df：\n{}'.format(Capacity_date_df.head(20)))
        Capacity_date_Calendar = pd.merge(Capacity_date_df, Calendar_df, how='left', on=['warehouse', 't'])
        print('Capacity_date_Calendar：\n{}'.format(Capacity_date_Calendar.head(20)))
        Capacity_date_Calendar.loc[Capacity_date_Calendar[Capacity_date_Calendar.sup == 1].index.tolist(), 'hours'] = 0
        Capacity_date_Calendar.drop(['sup'], axis=1, inplace=True)

        for i in range(Capacity_date_Calendar.shape[0]):
            date_in_Capacity = datetime.datetime.strptime(Capacity_date_Calendar.loc[i, 't'], "%Y-%m-%d").date()
            date_to_t = (date_in_Capacity - now_time).days
            Capacity_date_Calendar.loc[i, 't'] = date_to_t
        pc = pd.DataFrame(Capacity_date_Calendar, columns=['warehouse', 'line', 'package', 'num', 'hours', 't'])
        print('pc：\n{}'.format(pc.head(20)))

        # for i in range(Capacity_date_df.shape[0]):
        #     for j in range(Calendar_df.shape[0]):
        #         if Calendar_df.loc[j, 'warehouse'] == Capacity_date_df.loc[i, 'warehouse'] and Calendar_df.loc[j, 't'] == Capacity_date_df.loc[i, 't']:
        #             Capacity_date_df.loc[i, 'hours'] = 0
        #     date_in_Capacity = datetime.datetime.strptime(Capacity_date_df.loc[i, 't'], "%Y-%m-%d").date()
        #     date_to_t = (date_in_Capacity - now_time).days
        #     Capacity_date_df.loc[i, 't'] = date_to_t
        # pc = pd.DataFrame(Capacity_date_df, columns=['warehouse', 'line', 'package', 'num', 'hours', 't'])
    elif category == 13:
        the_first_week_date = datetime.datetime.strptime(list_date[0], "%Y-%m-%d").date()
        days_before_date = the_first_week_date + datetime.timedelta(days=-7)  # 第一周的前一周日期(特殊情况) # TODO(新增标记)
        days_before = days_before_date.strftime('%Y-%m-%d')  # str形式的日期
        total_list_date = list_date.copy()  # 新生成一个附带前一周的列表
        total_list_date.insert(0, days_before)
        # data_capacity 和 Calendar_df 各加一个辅助列sup
        data_capacity['sup'] = 1
        df_date = pd.DataFrame(total_list_date, columns=['t'])
        df_date['sup'] = 1
        Capacity_date_df = pd.merge(data_capacity, df_date, how='outer', on='sup')
        Capacity_date_df.drop(['sup'], axis=1, inplace=True)
        for i in range(Capacity_date_df.shape[0]):
            the_week_date = datetime.datetime.strptime(Capacity_date_df.loc[i, 't'], "%Y-%m-%d").date()
            the_hour = Capacity_date_df.loc[i, 'hours']
            the_warehouse = Capacity_date_df.loc[i, 'warehouse']
            rest_day_num = 0
            for j in range(Calendar_df.shape[0]):
                if Calendar_df.loc[j, 'warehouse'] == the_warehouse:
                    rest_date = datetime.datetime.strptime(Calendar_df.loc[j, 't'], "%Y-%m-%d").date()
                    dis_day = (the_week_date - rest_date).days
                    if dis_day <= 6 and dis_day >= 0:
                        rest_day_num = rest_day_num + 1
            week_hours = (7-rest_day_num) * the_hour
            the_t = (the_week_date - the_first_week_date).days // 7 + 1
            if the_t == 0:
                the_t = -1
            Capacity_date_df.loc[i, 'hours'] = week_hours
            Capacity_date_df.loc[i, 't'] = the_t
        pc = pd.DataFrame(Capacity_date_df, columns=['warehouse', 'line', 'package', 'num', 'hours', 't'])

        '''
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
        '''
    print('(capacity)产能数据\n{0}'.format(pc.head()))
    return pc
