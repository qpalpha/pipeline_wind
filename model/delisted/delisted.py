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

class delisted(WindBase):
    
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.file_name = self.ini.findString('delisted~Name')
        try:
            self.StartDate = self.ini.findInt('delisted~StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
        self.sql = "SELECT \
                    WINDDF.ASHAREPREVIOUSNAME.S_INFO_WINDCODE AS TICKER, \
                    WINDDF.ASHAREPREVIOUSNAME.BEGINDATE, \
                    WINDDF.ASHAREPREVIOUSNAME.ENDDATE, \
                    WINDDF.ASHAREPREVIOUSNAME.ANN_DT, \
                    WINDDF.ASHAREPREVIOUSNAME.S_INFO_NAME \
                    FROM \
                    WINDDF.ASHAREPREVIOUSNAME \
                    WHERE \
                    WINDDF.ASHAREPREVIOUSNAME.CHANGEREASON = 200036000 \
                    ORDER BY \
                    WINDDF.ASHAREPREVIOUSNAME.BEGINDATE ASC"

        
    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])           
        raw_data.fillna(20990101,inplace=True)
        raw_data['BEGINDATE']       = raw_data['BEGINDATE'].astype(int)
        raw_data['ENDDATE']         = raw_data['ENDDATE'].astype(int)
        date_list                   = dates.get_dates(self.StartDate,self.EndDate)
        data_df                     = pd.DataFrame()
        for ii in date_list:
            stock_list              = raw_data.loc[(raw_data['BEGINDATE'] <= ii)&(raw_data['ENDDATE'] >= ii),'TICKER']
            stock_df                = pd.DataFrame(np.ones(stock_list.shape).reshape([1,-1]),index = [ii], columns = stock_list)
            data_df                 = pd.concat([data_df, stock_df], axis = 0)
        self.df_data = data_df         
                
    def saveFile(self):
        try:
            file_dir = self.ini.findString('delisted~Outdir')
        except:
            file_dir = './'
        df_data                     = self.df_data
        filename                    = file_dir + '/' + self.file_name + '.bin'
        self.saveBinFile(df_data,filename)
        

if __name__ == '__main__':
    try:
        fini = sys.argv[1]
    except:
        fini = 'delisted.ini'
    a = delisted(fini)
    a.run()