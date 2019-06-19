#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 18 19:04:16 2019

@author: lixiang
"""

#%% Import part

import sys
sys.path.append('../../common')
from qp import *

import numpy as np
import pandas as pd
import os
import time
import datetime

#%%
def all_trade_dates():
#    file = '/qp/data/platform_objects/core/trade.date.txt'
    file = '/e/qp.test/data1/trade.date.txt'
    return pd.read_csv(file,header=None,squeeze=True).values

def _date_offset(date,offset=-1):
    dates = all_trade_dates()
    return dates[np.where(dates<=date)[0][-1]+offset]

def get_dates(sdate=None,edate=None,window=None,dates=None,return_type='int'):
    '''
    return_type : str
        'int'/'str'
    '''
    if dates is None:
        if (sdate is not None) and (edate is not None):
            dates = all_trade_dates()
            dates = dates[(dates>=sdate) & (dates<=edate)]
        elif (sdate is None) and (edate is not None):
            sdate = _date_offset(edate,offset=-window)
            dates = get_dates(sdate=sdate,edate=edate)
        elif (edate is None) and (sdate is not None):
            edate = _date_offset(sdate,offset=window)
            dates = get_dates(sdate=sdate,edate=edate)
    if return_type=='str':
        dates = [str(dt) for dt in dates]
    return dates

def today(return_type='int'):
    date = datetime.datetime.now().strftime('%Y%m%d')
    if return_type =='int':
        date = int(date) 
    return date

def date_offset(date,offset=1):
    dates = all_trade_dates()
    return dates[np.where(dates>=date)[0][0]+offset]

def pre_date_offset(date,offset=-1):
    if date is None:
        return None
    else:
        date = int(date)
    dates = all_trade_dates()
    return dates[np.where(dates<=date)[0][-1]+offset]
      