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

class split(WindBase):
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.file_name = self.ini.findString('Name')
        try:
            self.StartDate = self.ini.findInt('StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
        self.sql = '''
            select  
                S_INFO_WINDCODE as ticker,
                CASH_DVD_PER_SH_PRE_TAX,
                EX_DT as DT
            from
                winddf.AShareDividend
            where
                EX_DT is not null and
                EX_DT>={} and 
                CASH_DVD_PER_SH_PRE_TAX>0
            order by 
                EX_DT,ticker
            '''.format(self.StartDate)

        
    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])
        names                       = raw_data.columns.values.tolist()
        names.remove('TICKER')
        names.remove('DT')
        self.df_data                = self.processDailyData(raw_data, indexname='DT',columnname='TICKER',dataname=names[0])+1
        self.df_data.fillna(1,inplace=True)
        
                
    def saveFile(self):
        try:
            file_dir = self.ini.findString('Outdir')
        except:
            file_dir = './'
        df_data                     = self.df_data
        filename                    = file_dir + '/' + self.file_name + '.bin'
        self.saveBinFile(df_data,filename)

if __name__ == '__main__':
    a = split('split.ini')
    a.run()