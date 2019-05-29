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

class dividend(WindBase):
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.file_name = self.ini.findString('dividend~Name')
        try:
            self.StartDate = self.ini.findInt('dividend~StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
        self.sql = '''
            select  
                S_INFO_WINDCODE as ticker,
                STK_DVD_PER_SH,
                EX_DT as DT
            from
                winddf.AShareDividend
            where
                EX_DT is not null and
                EX_DT>={} and 
                STK_DVD_PER_SH>0
            order by 
                EX_DT,ticker
            '''.format(self.StartDate)

        
    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])
        names                       = raw_data.columns.values.tolist()
        names.remove('TICKER')
        names.remove('DT')
        self.df_data                = self.processDailyData(raw_data, indexname='DT',columnname='TICKER',dataname=names[0] )
                
    def saveFile(self):
        try:
            file_dir = self.ini.findString('dividend~Outdir')
        except:
            file_dir = './'
        df_data                     = self.df_data
        filename                    = file_dir + '/' + self.file_name + '.bin'
        self.saveBinFile(df_data,filename)

if __name__ == '__main__':
    a = dividend('dividend.ini')
    a.run()