# -*- coding: utf-8 -*-
"""
Created on Wed May 29 13:23:25 2019

@author: lixiang5
"""

from ini import Ini
import pandas as pd

def save_binary_matrix(fname,values,index,columns):
    df = pd.DataFrame(values,index=index,columns=columns)
    df.to_csv(fname)

def readm2df(fname):
    return pd.read_csv(fname,index_col=0)


if __name__=='__main__':
    data = readm2df('../data/split.bin')
    