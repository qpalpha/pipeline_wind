# -*- coding: utf-8 -*-
import sys
sys.path.append('../../common')
import cx_Oracle
import pandas as pd
import os
try:
    from qp import *
except:
    from ini import *
import datetime
from WindBase import *
import numpy as np

class TradeDate(WindBase):
    
    def __init__(self, ini_file = ''):
        WindBase.__init__(self, ini_file)
        self.type = 'TradeDate'
        try:
            StartDate = self.ini.findInt('TradeDate~StartDate')
        except:
            StartDate = 20070101
        self.sql = "SELECT \
                    WINDDF.ASHARECALENDAR.TRADE_DAYS \
                    FROM \
                    WINDDF.ASHARECALENDAR \
                    WHERE \
                    WINDDF.ASHARECALENDAR.S_INFO_EXCHMARKET = 'SSE' AND \
                    WINDDF.ASHARECALENDAR.TRADE_DAYS > %d \
                    ORDER BY \
                    WINDDF.ASHARECALENDAR.TRADE_DAYS ASC" % StartDate

        
    def processData(self):
        pass
                
                
    def saveFile(self):
        try:
            file_dir = self.ini.findString('TradeDate~OutFile')
        except:
            file_dir = './%s.txt' % self.type
        self.my_data_pd.to_csv(file_dir,header = False,index = False)
                

if __name__ == '__main__':
    a = TradeDate('TradeDate.ini')
    a.run()