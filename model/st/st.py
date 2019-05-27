# -*- coding: utf-8 -*-

import cx_Oracle
import pandas as pd
import os
from qp import Ini
import datetime
import sys
sys.path.append('../../common')
from WindBase import *
import numpy as np
import dates
import datetime

class st(WindBase):
    
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type = 'st'
        try:
            self.StartDate = self.ini.findInt('st~StartDate')
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
                    WINDDF.ASHAREPREVIOUSNAME.CHANGEREASON = 200002000 OR \
                    WINDDF.ASHAREPREVIOUSNAME.CHANGEREASON = 200004000 \
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
            file_dir = self.ini.findString('st~Outdir')
        except:
            file_dir = './'
        df_data                     = self.df_data
        date_index                  = [str(ii) for ii in df_data.index.values]
        stock_columns               = list(df_data.columns.values)
        filename                    = file_dir + '/' + 'st' + '.bin'
        save_binary_matrix(filename, df_data.values, date_index, stock_columns)
        print('Save File:%s' % filename)
    
                

if __name__ == '__main__':
    a = st('st.ini')
    a.run()