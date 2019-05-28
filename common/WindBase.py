# -*- coding: utf-8 -*-

import cx_Oracle
import pandas as pd
import os
from qp import *
import datetime

class WindBase():
    
    stocklist = pd.DataFrame(columns = ['StartDate','EndDate','Ticker','Type','Info'])
    
    def __init__(self, ini_file = ''):
        try:
            self.ini = Ini(ini_file)
        except:
            print('No ini file.')
        self.conn = cx_Oracle.connect('windfinc/Abc123@192.168.142.83/van')#这里的顺序是用户名/密码@oracleserver的ip地址/数据库名字
        os.system("export NLS_LANG=AMERICAN_AMERICA.ZHS16GBK") #add this to .bash_profile
        

        
    def getDatabaseData(self):
        self.my_data_pd = pd.read_sql(self.sql, self.conn)
        return self.my_data_pd
        
        
    def convertWindCode(self,ps):
        code = ps.str.slice(0,6)
        return code
    
    def convertTime2str(self, ps):
        data = [ii.strftime('%Y%m%d') for _,ii in ps.iteritems()]
        return data
    
    def shiftTime(self, ps, expire_day):
        data = [(ii+datetime.timedelta(expire_day)).strftime('%Y%m%d') for _,ii in ps.iteritems()]
        return data
        
    def processData(self):
        pass
                
    def saveFile(self):
        try:
            file_dir = self.ini.findString('OutDir')
        except:
            file_dir = ''
        self.stocklist.to_csv(file_dir + self.type + '.csv',index = False)

    def getQPData(self, fname, days):
        # get adjust Factor from QP DAta
        Init('/qp/data/ini/cn.eq.base.ini')
        date = int(datetime.date.today().strftime('%Y%m%d'))
        set_now(date, 1500)
        adj = QPData(fname)
        adj_pd = adj.history_matrix_pd(days, Constants.Yesterday, UnivType.All)
        return adj_pd
    
    def processBeginEndData(self, data_pd,StartDate,EndDate):
        raw_data                    = data_pd
        raw_data                    = raw_data[['TICKER','BEGINDATE','ENDDATE']]
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])           
        raw_data.fillna(20990101,inplace=True)
        raw_data['BEGINDATE']       = raw_data['BEGINDATE'].astype(int)
        raw_data['ENDDATE']         = raw_data['ENDDATE'].astype(int)
        date_list                   = dates.get_dates(StartDate,EndDate)
        data_df                     = pd.DataFrame()
        data_array                  = np.array([[],[]], ndmin= 2)
        for ii in date_list:
            stock_list              = raw_data.loc[(raw_data['BEGINDATE'] <= ii)&(raw_data['ENDDATE'] >= ii),'TICKER']
            date                    = np.tile(ii,stock_list.shape)
            idx                     = np.vstack([stock_list, date])
            data_array              = np.hstack([data_array, idx])
        index                       = pd.MultiIndex.from_arrays(data_array)
        data_df                     = pd.DataFrame(np.ones(len(index)),index = index)
        data_mat                    = data_df.unstack(level=0)
        data_mat.columns            = [ticker for ii, ticker in data_mat.columns]
        return data_mat
    
    def prodf(self, raw_df,columnname='',dataname='' ):
        new_df                      = pd.DataFrame(raw_df[dataname].values.reshape([1,-1]), columns = raw_df[columnname])
        return new_df
    
    def processDailyData(self, data, indexname='DT',columnname='TICKER',dataname='' ):
        raw_data                    = data[[indexname,columnname,dataname]]
        df_data                     = raw_data.groupby([indexname]).apply(self.prodf,columnname,dataname)
        df_data.reset_index(level=1,drop=True,inplace = True)
        return df_data
    
    def saveBinFile(self,data, filename):
        df_data                     = data
        date_index                  = [str(ii) for ii in df_data.index.values]
        stock_columns               = list(df_data.columns.values)
        save_binary_matrix(filename, df_data.values, date_index, stock_columns)
        print('Save File:%s' % filename)
    
        # caculate time cost
    def timeStart(self):
        self.__starttime = datetime.datetime.now()
        print('Start Timing.')
        
    def timeEnd(self):
        self.__endtime = datetime.datetime.now()
        exc_time = self.__endtime - self.__starttime
        print('Time cost : %f second'  % exc_time.seconds)

    def run(self):
        self.timeStart()
        self.getDatabaseData()
        self.processData()
        self.saveFile()
        self.timeEnd()