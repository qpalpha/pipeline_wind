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

class Index(WindBase):
    
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type       = 'Index'
        self.index_name = self.ini.findString('Index~Name')
        self.index_code = self.ini.findInt('Index~Code')
        try:
            self.StartDate = self.ini.findString('Index~StartDate')
        except:
            self.StartDate = 20070101
        self.EndDate = dates.today()
        self.sql = "SELECT \
                    WINDDF.AINDEXMEMBERS.S_CON_WINDCODE AS TICKER, \
                    WINDDF.AINDEXMEMBERS.S_CON_INDATE AS BEGINDATE, \
                    WINDDF.AINDEXMEMBERS.S_CON_OUTDATE AS ENDDATE \
                    FROM \
                    WINDDF.AINDEXMEMBERS  \
                    WHERE \
                    WINDDF.AINDEXMEMBERS.S_INFO_WINDCODE = '%s' \
                    ORDER BY \
                    BEGINDATE ASC" % self.index_code

    def processData(self):
        raw_data                    = self.my_data_pd
        data_mat                    = self.processBeginEndData(raw_data,self.StartDate,self.EndDate)
        self.df_data = data_mat         
        
        
    def saveFile(self):
        try:
            file_dir = self.ini.findString('Index~Outdir')
        except:
            file_dir = './'
        filename                    = file_dir + '/' + self.index_name + '.bin'
        df_data                     = self.df_data
        self.saveBinFile(df_data,filename)


if __name__ == '__main__':
    try:
        fini = sys.argv[1]
    except:
        fini = 'csi300.ini'
    a = Index(fini)
    a.run()