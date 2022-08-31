# -*- coding: utf-8 -*-
# @Time    : 2022-08-29 19:22
# @Author  : zhoujian.Pray
# @FileName: read_sql.py
# @Software: PyCharm
import pandas as pd
import sqlalchemy


# 创建连接
# 用户名:密码@地址:端口/数据库名称
engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@10.28.134.110:3306/warehouse')

# 1) 数据库导出
# 编写sql代码,执行sql代码,获取返回的值
sql = '''
select * from warehouse_type
where warehouse_type.发货仓库 = 'D301'
'''
df = pd.read_sql(sql, engine)

print(df.head())

# 2) 数据库写入
df["发货仓库"] = 'DDD'
df_new_line = df
df_new_line.to_sql('warehouse_type', con=engine, index=False, if_exists='append')
sql2 = '''
select * from warehouse_type
where warehouse_type.发货仓库 = 'DDD'
'''
df2 = pd.read_sql(sql2, engine)
print(df2.head())
