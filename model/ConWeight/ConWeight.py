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

class ConWeight(WindBase):
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.name             = self.ini.findString('Name')
        self.index_ticker     = self.ini.findString('IndexTicker')
        try:
            self.StartDate          = self.ini.findInt('StartDate')
        except:
            self.StartDate          = 20080101
        self.StartDate2             = dates._date_offset(self.StartDate,-31)
        self.EndDate                = dates.today()
        self.sql                    = '''
        select 
            s_con_windcode as ticker,
            trade_dt as dt,
            i_weight as wt
        from 
            winddf.aindexhs300freeweight
        where 
            s_info_windcode='{0}' and 
            trade_dt>={1}
        order by 
            trade_dt desc,s_con_windcode
        '''.format(self.index_ticker,self.StartDate2)
        


    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])
        trade_dates                 = dates.get_dates(sdate=self.StartDate,edate=self.EndDate,return_type='str')
        trade_dates2                = dates.get_dates(sdate=self.StartDate2,edate=self.EndDate,return_type='str')
        trade_dates_ret             = dates.get_dates(sdate=dates._date_offset(self.StartDate2),\
                                                      edate=dates._date_offset(self.EndDate),return_type='str')
        tickers                     = raw_data['TICKER'].drop_duplicates().values
        dts                         = raw_data['DT'].drop_duplicates()
        data_mat2                   = pd.DataFrame(index=trade_dates2,columns=tickers)
        for dt in dts:
            data_dt                 = raw_data[raw_data['DT']==dt]
            data_mat2.loc[dt,data_dt['TICKER']] = data_dt['WT'].values/100
        # Stock returns
        rets                        = asset_returns(tickers=tickers,dates=trade_dates_ret).values
        # Fill nans
        notnull_sindex              = np.where(data_mat2.notnull().any(axis=1))[0]
        notnull_eindex              = np.append(notnull_sindex[1:],([data_mat2.shape[0]]))
        for si,ei in zip(notnull_sindex,notnull_eindex):
            ret_i                   = rets[si:ei,:]
            ret_i[0,:]              = 0
            multiplier              = np.cumprod(1+ret_i,axis=0)
            data_i                  = data_mat2.iloc[si:ei,:].fillna(method='ffill').values
            data_mat2.iloc[si:ei,:] = data_i*multiplier
        # Capture data of trade_dates
        data_mat                    = data_mat2.loc[trade_dates,:]
        data_mat                    = data_mat.div(data_mat.sum(axis=1),axis=0)
        self.df_data = data_mat         
        
        
    def saveFile(self):
        try:
            file_dir = self.ini.findString('Outdir')
        except:
            file_dir = './'
        filename                    = file_dir + '/' + self.name + '.bin'
        df_data                     = self.screen_estu(self.df_data)
        self.saveBinFile(df_data,filename)


if __name__ == '__main__':
    try:
        fini = sys.argv[1]
    except:
        fini = 'csi500_wt.ini'
    a = ConWeight(fini)
    a.run()