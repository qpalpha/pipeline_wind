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

class EODIndex(WindBase):
    
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type = 'EODIndex'
        try:
            StartDate = self.ini.findInt('EODIndex~StartDate')
        except:
            StartDate = 20070101
        index_code = self.ini.findStringVec('EODIndex~IndexCode')
        index_name = self.ini.findStringVec('EODIndex~IndexName')
        self.index_dict = dict([(index_code[ii],index_name[ii]) for ii in range(len(index_code))])
        code_str = "'"
        for ii in index_code:
            code_str   = code_str + ii + "','"
        code_str  = code_str[:-2]
        self.sql = "SELECT \
                    WINDDF.AINDEXEODPRICES.TRADE_DT AS DT, \
                    WINDDF.AINDEXEODPRICES.S_INFO_WINDCODE AS TICKER, \
                    WINDDF.AINDEXEODPRICES.S_DQ_OPEN AS IOPEN, \
                    WINDDF.AINDEXEODPRICES.S_DQ_HIGH AS IHIGH, \
                    WINDDF.AINDEXEODPRICES.S_DQ_LOW AS ILOW, \
                    WINDDF.AINDEXEODPRICES.S_DQ_CLOSE AS ICLOSE, \
                    WINDDF.AINDEXEODPRICES.S_DQ_CHANGE AS ICHANGE, \
                    WINDDF.AINDEXEODPRICES.S_DQ_PCTCHANGE AS IPCTCHANGE, \
                    WINDDF.AINDEXEODPRICES.S_DQ_VOLUME AS IVOLUME, \
                    WINDDF.AINDEXEODPRICES.S_DQ_AMOUNT AS IAMOUNT \
                    FROM \
                    WINDDF.AINDEXEODPRICES \
                    WHERE \
                    WINDDF.AINDEXEODPRICES.TRADE_DT >= %s AND \
                    WINDDF.AINDEXEODPRICES.S_INFO_WINDCODE IN (%s) \
                    ORDER BY \
                    WINDDF.AINDEXEODPRICES.TRADE_DT ASC, \
                    WINDDF.AINDEXEODPRICES.S_INFO_WINDCODE ASC" % (StartDate,code_str)

        
    def processData(self):
        raw_data                    = self.my_data_pd
        raw_data['TICKER']          = [self.index_dict[ticker] for ticker in  raw_data['TICKER'].values]
        names                       = raw_data.columns.values.tolist()
        names.remove('TICKER')
        names.remove('DT')
        for ii in names:
            self.df_data                = self.processDailyData(raw_data, indexname='DT',columnname='TICKER',dataname=ii )
            self.saveFile(ii)
                
                
    def saveFile(self, name=''):
        try:
            file_dir = self.ini.findString('EODIndex~Outdir')
        except:
            file_dir = './'
        df_data                     = self.screen_estu(self.df_data)
        date_index                  = [str(ii) for ii in df_data.index.values]
        stock_columns               = list(df_data.columns.values)
        filename                    = file_dir + '/' + name.lower() + '.bin'
        save_binary_matrix(filename, df_data.values, date_index, stock_columns)
        print('Save File:%s' % filename)
    
    def run(self):
        self.getDatabaseData()
        self.processData()
                

if __name__ == '__main__':
    try:
        fini = sys.argv[1]
    except:
        fini = 'EODIndex.ini'
    a = EODIndex(fini)
    a.run()