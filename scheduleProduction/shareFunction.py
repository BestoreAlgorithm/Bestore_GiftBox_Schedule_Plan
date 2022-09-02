# -*- coding: utf-8 -*-
# @Time    : 2022-08-17 15:50
# @Author  : zhoujian.Pray
# @FileName: shareFunction.py
# @Software: PyCharm
import datetime


def data_list_create(days, now_time, category):
    if category == 7:
        # 规划期的第一天由当天更改为明天
        list_date_time = []  # 日期列表
        delta = datetime.timedelta(days=1)  # 延迟日期
        the_first_date = now_time + delta  # 规划日第一天日期为当天的后一天
        list_date_time.append(the_first_date)

        list_date = []  # 日期字符串列表
        str_date = the_first_date.strftime('%Y-%m-%d')
        list_date.append(str_date)
        day_num = days - 1
        for i in range(day_num):
            delta = datetime.timedelta(days=1)
            days_after = list_date_time[i] + delta
            list_date_time.append(days_after)
            str_date = days_after.strftime('%Y-%m-%d')
            list_date.append(str_date)
        return list_date_time, list_date
    elif category == 13:
        # 日期列表
        list_date_time = []  # 暂存容器，datetime形式的日期列表
        list_date = []  # 字符串形式的日期列表
        # 日期处理：生成13周的str日期和date日期
        week_day = now_time.weekday() + 1  # 当日星期几判断（其中周一的索引为 0，周日为 6）
        interval_to_weekend = 7 - week_day
        current_weekend_date = now_time + datetime.timedelta(days=interval_to_weekend)
        print('now_time: {}\n week_day: {}\n , interval_to_weekend: {}\n  days_after: {}'.
              format(now_time, week_day, interval_to_weekend, current_weekend_date))
        list_date_time.append(current_weekend_date)  # datetime形式的日期
        current_weekend_date_str = current_weekend_date.strftime('%Y-%m-%d')  # string形式的日期
        list_date.append(current_weekend_date_str)  # 当前周的周日日期（字符串列表）
        for i in range(days - 1):
            current_weekend_date = list_date_time[i] + datetime.timedelta(days=7)
            list_date_time.append(current_weekend_date)
            current_weekend_date_str = current_weekend_date.strftime('%Y-%m-%d')
            list_date.append(current_weekend_date_str)
        print('now_time:\n {}\n list_date_time:\n {}\n list_date:\n {}\n'.format(now_time, list_date_time, list_date))
        return list_date_time, list_date
