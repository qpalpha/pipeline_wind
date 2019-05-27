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