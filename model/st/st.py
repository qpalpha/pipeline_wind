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

class st(WindBase):
    
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type = 'st'
        try:
            self.StartDate = self.ini.findInt('st~StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
        self.sql  = "SELECT \
                    WINDDF.ASHAREST.S_INFO_WINDCODE AS TICKER, \
                    WINDDF.ASHAREST.ENTRY_DT AS BEGINDATE, \
                    WINDDF.ASHAREST.REMOVE_DT AS ENDDATE, \
                    WINDDF.ASHAREST.S_TYPE_ST \
                    FROM \
                    WINDDF.ASHAREST \
                    WHERE \
                    WINDDF.ASHAREST.S_TYPE_ST IN ('S','Z','P','X') \
                    ORDER BY \
                    BEGINDATE ASC"

        
    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['ENDDATE']         = [dates.pre_date_offset(ii,-1) for ii in raw_data['ENDDATE'].values]
        data_df                    = self.processBeginEndData(raw_data,self.StartDate,self.EndDate)
        self.df_data = data_df         
                
    def saveFile(self):
        try:
            file_dir = self.ini.findString('st~Outdir')
        except:
            file_dir = './'
        df_data                     = self.screen_estu(self.df_data)
        filename                    = file_dir + '/' + 'st' + '.bin'
        df_data                     = self.mergeBin(filename,df_data)
        self.saveBinFile(df_data,filename)
    
                

if __name__ == '__main__':
    a = st('st.ini')
    a.run()