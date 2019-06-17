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

class estu_r(WindBase):
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type = 'estu'
        try:
            self.StartDate = self.ini.findInt('StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
        self.sql = "SELECT \
            WINDDF.ASHAREEODPRICES.S_INFO_WINDCODE AS TICKER, \
            WINDDF.ASHAREEODPRICES.TRADE_DT AS DT, \
            WINDDF.ASHAREEODPRICES.S_DQ_AMOUNT AS AMOUNT \
            FROM \
            WINDDF.ASHAREEODPRICES \
            WHERE \
            WINDDF.ASHAREEODPRICES.TRADE_DT > %s \
            ORDER BY \
            WINDDF.ASHAREEODPRICES.TRADE_DT ASC, \
            WINDDF.ASHAREEODPRICES.S_INFO_WINDCODE ASC" % self.StartDate
        


    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])
        names                       = raw_data.columns.values.tolist()
        names.remove('TICKER')
        names.remove('DT')
        amount                      = self.processDailyData(raw_data, indexname='DT',columnname='TICKER',dataname='AMOUNT' )
        estu                        = load_data_dict('estu.a',fini='../../ini/dir.ini',dates_type='str')
        stop                        = load_data_dict('stop',fini='../../ini/dir.ini',dates_type='str')
        estu[stop==1]               = np.nan
        estu[amount!=amount]        = np.nan
        estu                        = estu.fillna(0)
        self.df_data                = estu

        
                
    def saveFile(self):
        try:
            file_dir = self.ini.findString('Outdir')
        except:
            file_dir = './'
        df_data                     = self.df_data
        filename                    = file_dir + '/' + 'estu.r.bin'
        df_data                     = self.mergeBin(filename,df_data)
        self.saveBinFile(df_data,filename)
    
                

if __name__ == '__main__':
    a = estu_r('estu.r.ini')
    a.run()