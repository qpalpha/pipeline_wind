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

class other(WindBase):
    def __init__(self, ini_file = '', time_type = 'morning'):
        WindBase.__init__(self, ini_file, time_type)
        self.type = 'estu'
        try:
            self.StartDate = self.ini.findInt('other~StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
        self.time_type = time_type
        self.today = str(dates.pre_date_offset(self.EndDate,0))
        self.index_list = self.ini.findStringVec('IndexName')

        
    def getDatabaseData(self):
        pass
        

    def processData(self):
        
        # an indicator for index instruments.|| 指数示性函数，除了最后四列指数为1，其余列均为0。
        data_mat  = load_data_dict('estu.a',fini=self.dict_ini,dates_type='str')
        data_mat.iloc[:,:] = 0
        for ii in self.index_list:
            data_mat[ii] = 1
        self.df_data = data_mat
        self.saveFile('indexflag')
        
        #总市值 market cap in RMB = unadjusted_close * 总股本
        shares  = load_data_dict('shares_outstanding',fini=self.dict_ini,dates_type='str')
        close  = load_data_dict('close',fini=self.dict_ini,dates_type='str')
        cap = close*shares
        self.df_data = cap
        self.saveFile('cap') 
        
        #float市值 market cap in RMB = unadjusted_close * shares_float
        shares_float  = load_data_dict('shares_float',fini=self.dict_ini,dates_type='str')
        close  = load_data_dict('close',fini=self.dict_ini,dates_type='str')
        cap2 = close *shares_float
        self.df_data = cap2
        self.saveFile('cap2')      
        
        # turnover
        shares1  = load_data_dict('shares_float',fini=self.dict_ini,dates_type='str')
        volume  = load_data_dict('volume',fini=self.dict_ini,dates_type='str')
        turnover = volume/shares1
        self.df_data = turnover
        self.saveFile('turnover') 
        
        # 21 day median liquidity  (yesterday's adjusted close price * 21 day median ajusted volume)
        adjclose   = load_data_dict('adjclose',fini=self.dict_ini,dates_type='str')
        adjvolume  = load_data_dict('adjvolume',fini=self.dict_ini,dates_type='str')
        adjvolume[adjvolume==0]= np.nan
        mean_volume = pd.rolling_median(adjvolume, 21, min_periods=1)
        mean_volume.fillna(0, inplace=True)
        liq21 = adjclose*mean_volume
        self.df_data = liq21
        self.saveFile('liq21') 
        
        mean_volume = pd.rolling_median(adjvolume, 101, min_periods=1)
        mean_volume.fillna(0, inplace=True)
        liq101 = adjclose*mean_volume
        self.df_data = liq101
        self.saveFile('liq101') 
        
        # 21 day median adjusted volume
        preadjvolume  = load_data_dict('preadjvolume',fini=self.dict_ini,dates_type='str')
        mean_volume = pd.rolling_median(preadjvolume, 21, min_periods=1)
        mean_volume.fillna(0, inplace=True)
        self.df_data = mean_volume
        self.saveFile('mdv21') 
        
        mean_volume = pd.rolling_median(preadjvolume, 101, min_periods=1)
        mean_volume.fillna(0, inplace=True)
        self.df_data = mean_volume
        self.saveFile('mdv101') 
            
#        # 股息率=一年的总派息额与当时市价的比例 eg.div.ratio['20180706','600336']= 0
#        self.sql = '''
#            select  
#                S_INFO_WINDCODE as ticker,
#                REPORT_PERIOD,
#                EX_DT as DT
#            from
#                winddf.AShareDividend
#            where
#                EX_DT is not null and
#                EX_DT>={} and 
#                CASH_DVD_PER_SH_PRE_TAX>0
#            order by 
#                EX_DT,ticker
#            '''.format(self.StartDate)
#        raw_data                    = pd.read_sql(self.sql, self.conn)
#        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])
#        raw_data                    = raw_data.groupby(['TICKER','DT']).apply(lambda x:x.iloc[0,:]).reset_index(drop=True)
#        raw_data['REPORT_PERIOD']   = np.floor(raw_data['REPORT_PERIOD'].astype(int)/1e4)
#        names                       = raw_data.columns.values.tolist()
#        names.remove('TICKER')
#        names.remove('DT')
#        div_year                    = self.processDailyData(raw_data, indexname='DT',columnname='TICKER',\
#                                                       dataname=names[0]).fillna(0)
#        dividend  = load_data_dict('dividend',fini=self.dict_ini,dates_type='str')
#        dividend2  = load_data_dict('dividend2',fini=self.dict_ini,dates_type='str')
#        
#        self.df_data = self.cal_div_ratio(div_year,dividend,shares,cap)
#        self.saveFile('div.ratio1') 
#        self.df_data = self.cal_div_ratio(div_year,dividend2,shares,cap)
#        self.saveFile('div.ratio2') 
        
        
    def cal_div_ratio(self,div_year,dividend,shares,cap):
        div_cash = (dividend*shares).fillna(0)
        
        div_cash_sum = pd.DataFrame(0,index=div_cash.index,columns=div_cash.columns)
        years = np.floor(div_cash.index.astype(int)/1e4).drop_duplicates()
        for year in years:
            div = div_cash[div_year==(year-1)].sum()
            idx = div_cash_sum.index[div_cash_sum.index.str.contains(str(int(year)))]
            div_cash_sum.loc[idx,:] = div_cash_sum.loc[idx,:].add(div)
            
        div_ratio = div_cash_sum/cap
        return div_ratio
        

    
    def saveFile(self, name=''):
        try:
            file_dir = self.ini.findString('other~Outdir')
        except:
            file_dir = './'
        filename                    = file_dir + '/' + name.lower() + '.bin'
        df_data                     = self.mergeBin(filename,self.df_data)
        df_data                     = self.screen_estu(df_data)
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
    except:
        fini = 'other.ini'
    a = other(fini)
    a.run()