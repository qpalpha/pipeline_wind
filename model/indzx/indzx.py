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
        
    def readsql(self, filename = 'sql'):
        f                           = open(filename)
        sql                         = f.read()
        f.close()
        return sql
    
    def processBeginEndData(self, data_pd,StartDate,EndDate,DataColumns=[]):
        raw_data                    = data_pd
        raw_data                    = raw_data[['TICKER','BEGINDATE','ENDDATE']+DataColumns]
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])           
        raw_data.fillna(20990101,inplace=True)
        raw_data['BEGINDATE']       = raw_data['BEGINDATE'].astype(int)
        raw_data['ENDDATE']         = raw_data['ENDDATE'].astype(int)
        date_list                   = dates.get_dates(StartDate,EndDate)
        data_df                     = pd.DataFrame()
        index_array                 = np.array([[],[]], ndmin= 2)
        data_array                  = np.array([])
        for ii in date_list:
            stock_list              = raw_data.loc[(raw_data['BEGINDATE'] <= ii)&(raw_data['ENDDATE'] >= ii),'TICKER']
            if len(DataColumns)>0:
                oneday_data         = raw_data.loc[(raw_data['BEGINDATE'] <= ii)&(raw_data['ENDDATE'] >= ii), DataColumns]
                data_array          = np.hstack([data_array, oneday_data.values.flat])
            date                    = np.tile(ii,stock_list.shape)
            idx                     = np.vstack([stock_list, date])
            index_array             = np.hstack([index_array, idx])
        index                       = pd.MultiIndex.from_arrays(index_array)
        if len(DataColumns)==0:
            data_array              = np.ones(len(index))
        data_df                     = pd.DataFrame(data_array,index = index)
        data_mat                    = data_df.unstack(level=0)
        data_mat.columns            = [ticker for ii, ticker in data_mat.columns]
        return data_mat


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
        df_data                     = self.df_data
        self.saveBinFile(df_data,filename)


if __name__ == '__main__':
    try:
        fini = sys.argv[1]
    except:
        fini = 'indzx.ini'
    a = indzx(fini)
    a.run()