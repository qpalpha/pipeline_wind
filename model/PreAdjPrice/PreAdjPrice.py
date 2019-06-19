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
    def __init__(self, ini_file = '', time_type = 'morning'):
        WindBase.__init__(self, ini_file, time_type)
        self.type = 'estu'
        try:
            self.StartDate = self.ini.findInt('PreAdjPrice~StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
        self.time_type = time_type
        self.today = str(dates.pre_date_offset(self.EndDate,0))

        
    def getDatabaseData(self):
        adj_factor  = load_data_dict('adjfactor',fini=self.dict_ini,dates_type='str')
        if self.time_type == 'morning':
            close_sql   =    'SELECT \
                            WINDDF.ASHAREEODPRICES.S_INFO_WINDCODE AS TICKER, \
                            WINDDF.ASHAREEODPRICES.TRADE_DT AS DT, \
                            WINDDF.ASHAREEODPRICES.S_DQ_CLOSE AS CLOSE \
                            FROM \
                            WINDDF.ASHAREEODPRICES \
                            WHERE \
                            WINDDF.ASHAREEODPRICES.TRADE_DT = %s \
                            ORDER BY \
                            DT ASC, \
                            TICKER ASC ' % str(dates.pre_date_offset(self.EndDate,-1))
            close       = pd.read_sql(close_sql, self.conn)
            close['TICKER']          = self.convertWindCode(close['TICKER'])
            close_df    = self.processDailyData(close, indexname='DT',columnname='TICKER',dataname='CLOSE' )
            split       = load_data_dict('split',fini=self.dict_ini,dates_type='str')
            div         = load_data_dict('dividend',fini=self.dict_ini,dates_type='str')
            div         = div.fillna(0)
            today       = str(dates.pre_date_offset(self.EndDate,0))
            yesterday   = str(dates.pre_date_offset(self.EndDate,-1))
            adj_factor.loc[today,:] = adj_factor.loc[yesterday,:]*split.loc[today,:]*(close_df.loc[yesterday,:]/(close_df.loc[yesterday,:]-div.loc[today,:]))
        pre_adj_factor = adj_factor/adj_factor.loc[today,:]
        self.pre_adj_factor = pre_adj_factor
        

    def processData(self):
        adj_factor  = load_data_dict('adjfactor',fini=self.dict_ini,dates_type='str')
        pre_adj_factor = self.pre_adj_factor
        names = ['OPEN','HIGH','LOW','CLOSE','VOLUME','VWAPSUM']
        for ii in names:
            adj_name = ii
            data = load_data_dict(adj_name,fini=self.dict_ini,dates_type='str')
            pre_adj_data = data * pre_adj_factor
            if ii == 'VOLUME':
                data = load_data_dict(ii,fini=self.dict_ini,dates_type='str')
                pre_adj_data = data / pre_adj_factor
            if self.time_type == 'morning':
                pre_adj_data.drop([self.today])
            self.df_data = pre_adj_data
            self.saveFile('preadj'+ii.lower())
        self.df_data = pre_adj_factor
        self.saveFile('preadjfactor')
            
                


    def saveFile(self, name=''):
        try:
            file_dir = self.ini.findString('PreAdjPrice~Outdir')
        except:
            file_dir = './'
        df_data                     = self.screen_estu(self.df_data)
        filename                    = file_dir + '/' + name.lower() + '.bin'
        df_data                     = self.mergeBin(filename,df_data)
        df_data = self.mergeIndex(name, df_data)
        self.saveBinFile(df_data,filename)
        
    def run(self):
        self.timeStart()
        self.getDatabaseData()
        self.processData()
        self.timeEnd()
    
                

if __name__ == '__main__':
    try:
        fini = sys.argv[1]
        time_type = sys.argv[2]
    except:
        fini = 'PreAdjPrice.ini'
        time_type = 'morning'
    a = estu_r(fini,time_type)
    a.run()