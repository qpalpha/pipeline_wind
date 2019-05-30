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

class stop(WindBase):
    
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type       = 'stop'
        self.stop_name = self.ini.findString('stop~Name')
        try:
            self.StartDate = self.ini.findInt('stop~StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
        self.sql = "SELECT \
                    WINDDF.ASHARETRADINGSUSPENSION.S_INFO_WINDCODE AS TICKER, \
                    WINDDF.ASHARETRADINGSUSPENSION.S_DQ_SUSPENDDATE AS BEGINDATE, \
                    WINDDF.ASHARETRADINGSUSPENSION.S_DQ_SUSPENDDATE AS ENDDATE \
                    FROM \
                    WINDDF.ASHARETRADINGSUSPENSION \
                    WHERE \
                    WINDDF.ASHARETRADINGSUSPENSION.S_DQ_SUSPENDTYPE = 444016000 AND \
                    WINDDF.ASHARETRADINGSUSPENSION.S_DQ_SUSPENDDATE >= %s \
                    ORDER BY \
                    BEGINDATE ASC" % self.StartDate

    def processData(self):
        raw_data                    = self.my_data_pd
        data_mat                    = self.processBeginEndData(raw_data,self.StartDate,self.EndDate)
        self.df_data = data_mat         
        
        
    def saveFile(self):
        try:
            file_dir = self.ini.findString('stop~Outdir')
        except:
            file_dir = './'
        filename                    = file_dir + '/' + self.stop_name + '.bin'
        df_data                     = self.screen_estu(self.df_data)
        self.saveBinFile(df_data,filename)


if __name__ == '__main__':
    try:
        fini = sys.argv[1]
    except:
        fini = 'stop.ini'
    a = stop(fini)
    a.run()