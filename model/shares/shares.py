# -*- coding: utf-8 -*-

import sys
sys.path.append('../../common')
import cx_Oracle
import pandas as pd
import os
try:
    from qp import *
except:
    from ini import *
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
                TRADE_DT as dt,
                TOT_SHR_TODAY as shares_outstanding,
                FLOAT_A_SHR_TODAY as shares_float,
                FREE_SHARES_TODAY as shares_free
            from 
                winddf.AShareEODDerivativeIndicator
            where 
                TRADE_DT>={}
            order by
                TRADE_DT,S_INFO_WINDCODE
            '''.format(self.StartDate)

        
    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])
        names                       = raw_data.columns.values.tolist()
        names.remove('TICKER')
        names.remove('DT')
        for ii in names:
            self.df_data            = self.processDailyData(raw_data, indexname='DT',columnname='TICKER',dataname=ii)*1e4
            self.saveFile(ii)
        
                
    def saveFile(self, name=''):
        try:
            file_dir = self.ini.findString('Outdir')
        except:
            file_dir = './'
        df_data                     = self.df_data
        date_index                  = [str(ii) for ii in df_data.index.values]
        stock_columns               = list(df_data.columns.values)
        filename                    = file_dir + '/' + name + '.bin'
        save_binary_matrix(filename, df_data.values, date_index, stock_columns)
        print('Save File:%s' % filename)

if __name__ == '__main__':
    a = shares('shares.ini')
    a.run()