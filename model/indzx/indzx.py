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

class indzx(WindBase):
    
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type                   = 'indzx'
        self.indzx_name             = self.ini.findString('indzx~Name')
        try:
            self.StartDate          = self.ini.findInt('indzx~StartDate')
        except:
            self.StartDate          = 20070101
        self.EndDate                = dates.today()
        sqlfile                     = self.ini.findString('indzx~SQLFile')
        self.sql                    = self.readsql(sqlfile)
        


    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['ZX_CODE']         = 11*1e+8 + raw_data['ZX_CODE'].str[3:4].apply(lambda x : ord(x) -87 if not x.isdigit() else int(x))*1e+6 + raw_data['ZX_CODE'].str[4:10].astype(int)
        data_mat                    = self.processBeginEndData(raw_data,self.StartDate,self.EndDate,['ZX_CODE'])
        self.df_data = data_mat         
        
        
    def saveFile(self):
        try:
            file_dir = self.ini.findString('indzx~Outdir')
        except:
            file_dir = './'
        filename                    = file_dir + '/' + self.indzx_name + '.bin'
        df_data                     = self.screen_estu(self.df_data)
        self.saveBinFile(df_data,filename)


if __name__ == '__main__':
    try:
        fini = sys.argv[1]
    except:
        fini = 'indzx.ini'
    a = indzx(fini)
    a.run()