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

class snew(WindBase):
    
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type           = 'snew'
        self.file_name      = self.ini.findString(self.type + '~Name')
        try:
            self.StartDate  = self.ini.findInt(self.type + '~StartDate')
        except:
            self.StartDate  = 20070101
        self.EndDate        = dates.today()
        self.sql = "SELECT \
                    WINDDF.ASHAREDESCRIPTION.S_INFO_WINDCODE AS TICKER, \
                    WINDDF.ASHAREDESCRIPTION.S_INFO_NAME, \
                    WINDDF.ASHAREDESCRIPTION.S_INFO_LISTBOARD , \
                    WINDDF.ASHAREDESCRIPTION.S_INFO_LISTDATE AS BEGINDATE, \
                    WINDDF.ASHAREDESCRIPTION.S_INFO_DELISTDATE AS ENDDATE, \
                    WINDDF.ASHAREDESCRIPTION.S_INFO_LISTBOARDNAME \
                    FROM \
                    WINDDF.ASHAREDESCRIPTION \
                    WHERE \
                    WINDDF.ASHAREDESCRIPTION.S_INFO_LISTDATE > 0 \
                    -- AND WINDDF.ASHAREDESCRIPTION.S_INFO_LISTBOARD <> 434009000"
        
    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['ENDDATE']         = [dates.date_offset(int(dt),120) for dt in raw_data['BEGINDATE'].values]
        data_mat                    = self.processBeginEndData(raw_data,self.StartDate,self.EndDate)
        self.df_data = data_mat        
                
    def saveFile(self):
        try:
            file_dir = self.ini.findString(self.type + '~Outdir')
        except:
            file_dir = './'
        df_data                     = self.screen_estu(self.df_data)
        filename                    = file_dir + '/' + self.file_name + '.bin'
        df_data                     = self.mergeBin(filename,df_data)
        self.saveBinFile(df_data,filename)
        

if __name__ == '__main__':
    try:
        fini = sys.argv[1]
    except:
        fini = 'snew.ini'
    a = snew(fini)
    a.run()