# -*- coding: utf-8 -*-
# @Time    : 2022-08-31 11:21
# @Author  : zhou_jian.Pray
# @FileName: data_structure.py.py
# @Software: PyCharm

import gurobipy as grb
import gurobipy as gp
from gurobipy import GRB

'''
引入Multidict、Tuplelist、Tupledict 三种数据结构的目的：
在建模过程中，经常要对带下标数据做挑选，不同下标的数据进行组合，在对多维下表不采用多重循环和if判断的条件下，
为了提高效率引入 gurobipy的数据结构
'''
#  Multidict、Tuplelist、Tupledict 三种数据结构

# 1）gurobipy.mutidict 用法
'''
multidict(data)函数提供将一个dict类型的对象data转化为tupledict。
如果data的value包含了N个元素，则该函数返回的N+1个对象，
第1个对象为data中的keys，后续对象为将N个value打散的tupledict。
'''
student, chinese, math, english = grb.multidict({
    'student1': [100, 120, 130],
    'student2': [120, 130, 110],
    'student3': [130, 140, 105],
    'student4': [140, 125, 135]
})
print('student: {}\n'.format(student))  # 字典的键
print('chinese: {}\n'.format(chinese))  # 语文成绩的字典
print('math: {}\n'.format(math))  # 数学成绩的字典
print('english: {}\n'.format(english))  # 英语成绩的字典

# 2）gurobipy.Tuplelist 用法
'''
2.1: tulpelist来自与python list的子类，所以向tuplelist中添加新元素和list的用法一样，有append、pop等方法
2.2: tuplelist：元组列表，list元素的tuple类型，可以高效地在元组列表中构建子列表。
2.3: 可以使用tuplelist对象的select方法进行元组检索，很想SQL语句中的select-where操作。
'''
tl = grb.tuplelist([(100, 120, 130), (120, 130, 110), (130, 140, 105), (140, 125, 135)])  # 创建tuplelist对象
print(tl)  # 输出整个tuplelist
print(tl.select(100, '*'))  # 取出第一个值是1的元素(method1)
print(tl.select(100, '*', '*'))  # 取出第一个值是1的元素(method2)
print(tl.select('*', 130))  # 取出第二个值是3的元素(method1)
print(tl.select('*', '*', 130))  # 取出第二个值是3的元素(method2)
tl.append((100, 120, 150))  # 添加一个元素
print(tl.select(100, '*'))
print([(x, y, z) for x, y, z in tl if x == 100])  # 对应的迭代语法
if (100, 120, 150) in tl:
    print("Tuple (100, 120, 150) is in tuplelist tl")

# 3）gurobipy.tupledict 用法
'''
3.1: tupledict是Python的dict的子类。由键值两部分组成。key为上文提到的tuplelist，value为Gurobi的变量Var类型
3.2: Gurobi变量一般都是tupledict类型。在实际应用中，通过将元组与每个Gurobi变量关联起来，可以有效地创建包含匹配变量子集的表达式。
3.3: tupledict的key（键）在内部的存储格式是tuplelist，因此可以使用tuplelist的select方法筛选。
'''
model = grb.Model()
# 定义变量的下标
'''
[x11, x12, x13,
 x21, x22, x23,
 x31, x32, x33]
'''
tl = [(1, 1), (1, 2), (1, 3),
      (2, 1), (2, 2), (2, 3),
      (3, 1), (3, 2), (3, 3)]
vars = model.addVars(tl, name="x")
model.update()
print('vars:\n {} \n vars.type: \n {}'.format(vars, type(vars)))

#  与tuplelist相同，使用select()函数可以筛选出符合条件的key的value
#  像dict一样，使用[]进行访问，但不可使用通配符，也就是每次只能选出一个元素
# 筛选元素
d = grb.tupledict([((1, 1), 'a'), ((1, 2), 'b'), ((2, 1), 'c'), ((2, 2), 'd')])
print(d.select())  # 显示所有元素
print(d.select(1, '*'))  # pattern筛选元素
print(d[1, 1])  # 下标访问 # d[1,'*'] #错误的使用

# 矩阵变量的创建
'''
Model.addMVar()
addMVar(shape, lb=0.0, ub=GRB.INFINITY, obj=0.0, vtype=GRB.CONTINUOUS, name="")
'''
# shape：矩阵向量的维度
# lb, ub：分别为变量的上下限（可选）
# obj：变量在目标函数表达式中的系数（可选）
# vtype：变量类型（可选）
model = grb.Model()
x = model.addMVar(10)  # 包含10个变量的一维矩阵变量
y = model.addMVar((3, 4), vtype=GRB.BINARY)  # 3x4的0-1变量矩阵
print('x: {}, y: {}'.format(x, y))
