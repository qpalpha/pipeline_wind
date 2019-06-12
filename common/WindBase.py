# -*- coding: utf-8 -*-

import cx_Oracle
import pandas as pd
import numpy as np
import os
from qp import *
import datetime
import dates as dts

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
    
    def processBeginEndData(self, data_pd,StartDate,EndDate,DataColumns=[]):
        raw_data                    = data_pd
        raw_data                    = raw_data[['TICKER','BEGINDATE','ENDDATE']+DataColumns]
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])           
        raw_data.fillna(20990101,inplace=True)
        raw_data['BEGINDATE']       = raw_data['BEGINDATE'].astype(int)
        raw_data['ENDDATE']         = raw_data['ENDDATE'].astype(int)
        date_list                   = dts.get_dates(StartDate,EndDate)
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
    
    def prodf(self, raw_df,columnname='',dataname='' ):
        print(raw_df.iloc[0,0])
        if raw_df.iloc[0,0] == '20190620':
            a=1
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
        
    def readsql(self, filename = 'sql'):
        f                           = open(filename)
        sql                         = f.read()
        f.close()
        return sql
    
        # caculate time cost
    def timeStart(self):
        self.__starttime = datetime.datetime.now()
        print('Start Timing.')
        
    def timeEnd(self):
        self.__endtime = datetime.datetime.now()
        exc_time = self.__endtime - self.__starttime
        print('Time cost : %f second'  % exc_time.seconds)
        
    def mergeData(self, data1, data2):
        date_list1 = data1.index.values
        date_list2 = data2.index.values
        date_list1_new = list(set(date_list1).difference(set(date_list2)))
        data = pd.concat([data1.loc[date_list1_new],data2],axis=0)
        return data
    
    def screen_estu(self,df_data,fillna=None):
        df_data.index               = df_data.index.astype(str)
        estu                        = load_data_dict('estu',dates_type='str')
        dates                       = list(estu.index&set(df_data.index))
        dates.sort()
        df_data                     = pd.DataFrame(df_data,columns=estu.columns,index=dates)
        if fillna is not None:
            df_data                 = df_data.fillna(fillna)
        df_data[~(estu.loc[dates,:]==1)] = np.nan
        return df_data

    def run(self):
        self.timeStart()
        self.getDatabaseData()
        self.processData()
        self.saveFile()
        self.timeEnd()
        
#%%
def load_data_dict(field,ini=None,fini=None,fill0=None,dates_type='int'):
    if fini is None:
#        fini = '../../common/cn.eq.base.ini'
        fini = '/qp/data/ini/cn.eq.base.ini'
    if ini is None:
        ini = Ini(fini)
    try:
        svec = ini.findStringVec('DataDictionary~'+field)
    except:
        raise Exception('[{}] is not in the DataDictionary!'.format(field))
    data = readm2df(svec[0].split(':')[1])
    if len(svec)>1:
        for ss in svec[1:]:
            data_next = readm2df(ss.split(':')[1])
            data = merge_and_update(data,data_next)
    if dates_type=='int': data.index = data.index.astype(int)
    elif dates_type=='str': data.index = data.index.astype(str)
    if fill0 is not None:
        data[data==0] = fill0
    return data

def merge_and_update(df1,df2):
    # dates
    dts1 = set(df1.index)
    dts2 = set(df2.index)
    dts_merge = list(set.union(dts1,dts2))
    dts_merge.sort()
    dts_new = list(dts2-dts1)
    # tickers
    df = pd.DataFrame(df1,index=dts_merge)
    df.loc[dts_new,:] = df2.loc[dts_new,:]
    return df

def asset_returns(tickers=None,dates=None,rolling_window=1,ret_type='realized'):
    if type(dates[0]) is str:
        dates = np.array([int(dt) for dt in dates])
    sdate = dates[0]
    edate = dates[-1]
    if ret_type=='realized':
        ssdate = dts._date_offset(sdate,offset=-rolling_window)
        sedate = dts._date_offset(edate,offset=-rolling_window)
        dates_all = dts.get_dates(ssdate,edate)
        dates0 = dates_all[dates_all<=sedate]
        dates1 = dates_all[dates_all>=sdate]
        index_dates = dates1
    elif ret_type=='future':
        esdate = dts._date_offset(sdate,offset=rolling_window)
        eedate = dts._date_offset(edate,offset=rolling_window)
        dates_all = dts.get_dates(sdate,eedate)
        dates0 = dates_all[dates_all<=edate]
        dates1 = dates_all[dates_all>=esdate]
        index_dates = dates0
    price = load_data_dict('adjusted_close',fill0=np.nan)
    price = price.loc[dates_all,:]
    if tickers is not None:
        price = price.loc[:,tickers]
    ret = price.loc[dates1,:].values/price.loc[dates0,:].values-1
    ret_df = pd.DataFrame(ret,index=index_dates,columns=price.columns)
    return ret_df.loc[dates,:]
