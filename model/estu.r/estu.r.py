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

    def getDatabaseData(self):
        estu                        = load_data_dict('estu')
        stop                        = load_data_dict('stop')
        estu[stop>0]                = np.nan
        self.my_data_pd             = estu

    def processData(self):
        self.df_data                = self.my_data_pd
        
                
    def saveFile(self):
        try:
            file_dir = self.ini.findString('Outdir')
        except:
            file_dir = './'
        df_data                     = self.df_data
        date_index                  = [str(ii) for ii in df_data.index.values]
        stock_columns               = list(df_data.columns.values)
        filename                    = file_dir + '/' + 'estu.r.bin'
        save_binary_matrix(filename, df_data.values, date_index, stock_columns)
        print('Save File:%s' % filename)
    
                

if __name__ == '__main__':
    a = estu_r('estu.r.ini')
    a.run()