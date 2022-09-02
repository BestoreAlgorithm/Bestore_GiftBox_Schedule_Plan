# -*- coding: utf-8 -*-
# @Time    : 2022-09-02 14:50
# @Author  : zhoujian.Pray
# @FileName: requieEnvironment.py
# @Software: PyCharm
import os
import platform
import sys
import subprocess

# 方法1
# pipreqs --encoding = utf-8 --force
project_root = os.path.dirname(os.path.realpath(__file__))  # 找到当前目录
# project_root = os.path.realpath(__file__)
print('当前目录' + project_root)

# 不同的系统，使用不同的命令语句
if platform.system() == 'Linux':
    command = sys.executable + ' -m pip freeze > ' + project_root + '/requirements.txt'
if platform.system() == 'Windows':
    command = '"' + sys.executable + '"' + ' -m pip freeze > "' + project_root + '\\requirements.txt"'
# 拼接生成requirements命令
print(command)
# os.system(command)  #路径有空格不管用
os.popen(command)  # 路径有空格，可用
# subprocess.call(command, shell=True)  #路径有空格，可用

