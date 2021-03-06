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

class EODPrice(WindBase):
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type = 'EODPrice'
        try:
            StartDate = self.ini.findInt('EODPrice~StartDate')
        except:
            StartDate = 20070101

        self.sql = "SELECT \
                    WINDDF.ASHAREEODPRICES.S_INFO_WINDCODE AS TICKER, \
                    WINDDF.ASHAREEODPRICES.TRADE_DT AS DT, \
                    WINDDF.ASHAREEODPRICES.S_DQ_OPEN AS OPEN, \
                    WINDDF.ASHAREEODPRICES.S_DQ_HIGH AS HIGH, \
                    WINDDF.ASHAREEODPRICES.S_DQ_LOW AS LOW, \
                    WINDDF.ASHAREEODPRICES.S_DQ_CLOSE AS CLOSE, \
                    WINDDF.ASHAREEODPRICES.S_DQ_CHANGE AS CHANGE, \
                    WINDDF.ASHAREEODPRICES.S_DQ_PCTCHANGE AS PCTCHANGE, \
                    WINDDF.ASHAREEODPRICES.S_DQ_VOLUME AS VOLUME, \
                    WINDDF.ASHAREEODPRICES.S_DQ_AMOUNT AS AMOUNT, \
                    WINDDF.ASHAREEODPRICES.S_DQ_ADJOPEN AS ADJOPEN, \
                    WINDDF.ASHAREEODPRICES.S_DQ_ADJHIGH AS ADJHIGH, \
                    WINDDF.ASHAREEODPRICES.S_DQ_ADJLOW AS ADJLOW, \
                    WINDDF.ASHAREEODPRICES.S_DQ_ADJCLOSE AS ADJCLOSE, \
                    WINDDF.ASHAREEODPRICES.S_DQ_VOLUME AS VOLUME, \
                    WINDDF.ASHAREEODPRICES.S_DQ_ADJFACTOR AS ADJFACTOR, \
                    WINDDF.ASHAREEODPRICES.S_DQ_AVGPRICE AS VWAPSUM \
                    FROM \
                    WINDDF.ASHAREEODPRICES \
                    WHERE \
                    WINDDF.ASHAREEODPRICES.TRADE_DT > %s \
                    ORDER BY \
                    WINDDF.ASHAREEODPRICES.TRADE_DT ASC, \
                    WINDDF.ASHAREEODPRICES.S_INFO_WINDCODE ASC" % StartDate

        
    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['TICKER']          = self.convertWindCode(raw_data['TICKER'])
        names                       = raw_data.columns.values.tolist()
        names.remove('TICKER')
        names.remove('DT')
        for ii in names:
            self.df_data                = self.processDailyData(raw_data, indexname='DT',columnname='TICKER',dataname=ii )
            if ii == 'ADJFACTOR':
                self.adjfactor = self.df_data.copy()
            elif ii == 'VOLUME':
                self.df_data = self.df_data*100
                self.volume = self.df_data.copy()
            elif ii == 'VWAPSUM':
                self.vwapsum = self.df_data.copy()
            self.saveFile(ii)
#        self.volume= load_data_dict('volume',fini=self.dict_ini,dates_type='str')
#        self.adjfactor= load_data_dict('adjfactor',fini=self.dict_ini,dates_type='str')
#        self.vwapsum= load_data_dict('vwapsum',fini=self.dict_ini,dates_type='str')
        adjvolume = self.volume/self.adjfactor
        self.df_data = adjvolume
        self.saveFile('ADJVOLUME')
        adjvwapsum = self.vwapsum*self.adjfactor
        self.df_data = adjvwapsum
        self.saveFile('ADJVWAPSUM')
                
                
    def saveFile(self, name=''):
        try:
            file_dir = self.ini.findString('EODPrice~Outdir')
        except:
            file_dir = './'
        filename                    = file_dir + '/' + name.lower() + '.bin'
        df_data                     = self.mergeBin(filename,self.df_data)
        df_data                     = self.screen_estu(df_data)
        if name in ['OPEN','HIGH','LOW','CLOSE','VWAPSUM']:
            df_data = self.mergeIndex(name, df_data)
            self.saveBinFile(df_data,filename)
            df_data                     = self.screen_estur(df_data,0)
            filename                    = file_dir + '/u' + name.lower() + '.bin'
        df_data = self.mergeIndex(name, df_data)
        self.saveBinFile(df_data,filename)
    
    def run(self):
        self.timeStart()
#        self.getDatabaseData()
        self.processData()
        self.timeEnd()
                

if __name__ == '__main__':
    try:
        fini = sys.argv[1]
    except:
#        fini = 'EODPrice.ini'
        fini = '/e/qp.test/pipeline_wind/ini/job.update.ini'
    a = EODPrice(fini)
    a.run()