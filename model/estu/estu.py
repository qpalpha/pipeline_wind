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

class estu(WindBase):
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type = 'estu'
        try:
            self.StartDate = self.ini.findInt('estu~StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
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
                    WINDDF.ASHAREDESCRIPTION.S_INFO_LISTDATE > 0 AND \
                    WINDDF.ASHAREDESCRIPTION.S_INFO_LISTBOARD <> 434009000"

        
    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data                    = raw_data[['TICKER','BEGINDATE','ENDDATE']]
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])           
        raw_data.fillna(20990101,inplace=True)
        raw_data['BEGINDATE']       = raw_data['BEGINDATE'].astype(int)
        raw_data['ENDDATE']         = raw_data['ENDDATE'].astype(int)
        date_list                   = dates.get_dates(self.StartDate,self.EndDate)
        data_df                     = pd.DataFrame()
        data_array                  = np.array([[],[]], ndmin= 2)
        for ii in date_list:
            stock_list              = raw_data.loc[(raw_data['BEGINDATE'] <= ii)&(raw_data['ENDDATE'] >= ii),'TICKER']
            date                    = np.tile(ii,stock_list.shape)
            idx                     = np.vstack([stock_list, date])
            data_array              = np.hstack([data_array, idx])
        index =     pd.MultiIndex.from_arrays(data_array)
        data_df                     = pd.DataFrame(np.ones(len(index)),index = index)
        data_mat                    = data_df.unstack(level=0)
        data_mat.columns            = [ticker for ii, ticker in data_mat.columns]
        self.df_data = data_mat         
                
    def saveFile(self):
        try:
            file_dir = self.ini.findString('estu~Outdir')
        except:
            file_dir = './'
        df_data                     = self.df_data
        date_index                  = [str(ii) for ii in df_data.index.values]
        stock_columns               = list(df_data.columns.values)
        filename                    = file_dir + '/' + 'estu' + '.bin'
        save_binary_matrix(filename, df_data.values, date_index, stock_columns)
        print('Save File:%s' % filename)
    
                

if __name__ == '__main__':
    a = estu('estu.ini')
    a.run()