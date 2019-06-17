#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 12 14:04:02 2019

@author: liuyun
"""
from qp import *
import numpy as np
data_new = readm2df('../data/st.bin')
data     = readm2df('/qp/data/platform_objects/core/st.bin')
#data_new = data_new.fillna(0)

stock_more = list(set(data_new.columns.values).difference(set(data.columns.values)))
print('stock_more:')
print(stock_more)
stock_less = list(set(data.columns.values).difference(set(data_new.columns.values)))
print('stock_less:')
print(stock_less)
stock_uni   = list(set(data.columns.values).intersection(set(data_new.columns.values)))
stock_uni.sort()

date_more = list(set(data_new.index.values).difference(set(data.index.values)))
print('date_more:')
print(date_more)
date_less = list(set(data.index.values).difference(set(data_new.index.values)))
print('date_less:')
print(date_less)
date_uni  = list(set(data.index.values).intersection(set(data_new.index.values)))
date_uni.sort()

DIF_TRES = 0.1
print('##diff test##')
data_new_uni = data_new.loc[date_uni,stock_uni]
data_uni = data.loc[date_uni,stock_uni]
data_diff = data_uni.sub(data_new_uni,fill_value=0)
index_sum = data_diff.sum(axis=1)
diff_sum = (~(np.abs(data_diff)<=DIF_TRES)).sum(axis=1)
print('min and max diff:')
print(index_sum.min())
print(index_sum.max())
diff_array= np.where(~(np.abs(data_diff)<=DIF_TRES))
diff_list = [[data_diff.index.values[diff_array[0][ii]],data_diff.columns.values[diff_array[1][ii]],
              data_new_uni.loc[data_diff.index.values[diff_array[0][ii]],data_diff.columns.values[diff_array[1][ii]]],
              data_uni.loc[data_diff.index.values[diff_array[0][ii]],data_diff.columns.values[diff_array[1][ii]]]] for ii in range(len(diff_array[0]))]
if not diff_list:
    print('data match')
else:
    print('diff date and stock:')
    print(diff_list[0])
    print(data_new_uni.loc[diff_list[0][0],diff_list[0][1]])
    print(data_uni.loc[diff_list[0][0],diff_list[0][1]])

data_diff2 = data_uni!=data_new_uni
index_sum = data_diff2.sum(axis=1)
print('max diff nan:')
print(index_sum.max())

# nan data
print('##nan test##')
nan_array= np.where(data_uni!=data_uni)
nan_list = [[data_diff.index.values[nan_array[0][ii]],data_diff.columns.values[nan_array[1][ii]] ] for ii in range(len(nan_array[0]))]
if not nan_list:
    print('data match')
else:
    print('nan date and stock:')
    print(nan_list[0])
    print(data_uni.loc[nan_list[0][0],nan_list[0][1]])