# -*- coding: utf-8 -*-

import sys
sys.path.append('../../common')
import cx_Oracle
import pandas as pd
import os
from qp import *
import datetime
from WindBase import *
import numpy as np
import dates
import warnings

warnings.filterwarnings('ignore')

class shares(WindBase):
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        try:
            self.StartDate = self.ini.findInt('StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
        self.sql = '''
            select 
                S_INFO_WINDCODE as ticker,
                CHANGE_DT1 as dt,
                TOT_SHR as shares_outstanding,
                FLOAT_SHR as shares_float,
                FLOAT_A_SHR as shares_float_a,
                ANN_DT
            from 
                winddf.AShareCapitalization
            order by
                CHANGE_DT,S_INFO_WINDCODE,ANN_DT
            '''
        
    def processData(self):
        raw_data                    = self.my_data_pd.drop_duplicates(['TICKER','DT'],keep='last')
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])
        raw_data['DT']              = raw_data['DT'].astype(int)
        # Data before and after self.StartDate
        raw_data_before             = raw_data[raw_data['DT']<=self.StartDate]
        raw_data_after              = raw_data[raw_data['DT']>self.StartDate]
        raw_data_before_last_day    = raw_data_before.groupby(['TICKER']).apply(lambda df:df.iloc[-1,:])
        raw_data_before_last_day['DT'] = self.StartDate
        raw_data                    = pd.concat([raw_data_before_last_day,raw_data_after])
        # names
        names                       = raw_data.columns.values.tolist()
        names.remove('TICKER')
        names.remove('DT')
        names.remove('ANN_DT')
        
        for ii in names:
            df_data                 = self.processDailyData(raw_data, indexname='DT',columnname='TICKER',dataname=ii)*1e4
            self.df_data            = df_data.fillna(method='ffill',axis=0)
            self.saveFile(ii)
        
    
    def saveFile(self, name=''):
        if name:
            try:
                file_dir = self.ini.findString('Outdir')
            except:
                file_dir = './'
            df_data                     = self.screen_estu(self.df_data)
            filename                    = file_dir + '/' + name.lower() + '.bin'
            df_data                     = self.mergeBin(filename,df_data)
            self.saveBinFile(df_data,filename)
    
    
if __name__ == '__main__':
    a = shares('shares.ini')
    a.run()