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
                    WINDDF.ASHAREEODPRICES.S_DQ_PCTCHANGE AS PCTCCHANGE, \
                    WINDDF.ASHAREEODPRICES.S_DQ_VOLUME AS VOLUME, \
                    WINDDF.ASHAREEODPRICES.S_DQ_AMOUNT AS AMOUNT, \
                    WINDDF.ASHAREEODPRICES.S_DQ_ADJOPEN AS ADJOPEN, \
                    WINDDF.ASHAREEODPRICES.S_DQ_ADJHIGH AS ADJHIGH, \
                    WINDDF.ASHAREEODPRICES.S_DQ_ADJLOW AS ADJLOW, \
                    WINDDF.ASHAREEODPRICES.S_DQ_ADJCLOSE AS ADJCLOSE, \
                    WINDDF.ASHAREEODPRICES.S_DQ_ADJFACTOR AS ADJFACTOR, \
                    WINDDF.ASHAREEODPRICES.S_DQ_AVGPRICE AS VWAPSUM, \
                    WINDDF.ASHAREEODPRICES.S_DQ_TRADESTATUS AS TRADESTATUS \
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
            self.saveFile(ii)
                
                
    def saveFile(self, name=''):
        try:
            file_dir = self.ini.findString('EODPrice~Outdir')
        except:
            file_dir = './'
        df_data                     = self.df_data
        date_index                  = [str(ii) for ii in df_data.index.values]
        stock_columns               = list(df_data.columns.values)
        filename                    = file_dir + '/' + name + '.bin'
        save_binary_matrix(filename, df_data.values, date_index, stock_columns)
        print('Save File:%s' % filename)
    
    def run(self):
        self.getDatabaseData()
        self.processData()
                

if __name__ == '__main__':
    a = EODPrice('EODPrice.ini')
    a.run()