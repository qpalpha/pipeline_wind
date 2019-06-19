#!/usr/bin/bash
#-*- coding:utf-8 -*-
####################################
#File Name:     daily.job.night.sh
#Author:        qp
#Mail:          
#Created Time:  2019-06-14 09:02:40
####################################

echo '----------------- [Morning] Basic Pipeline Starts -----------------'

MAIN_DIR=/e/qp.test/pipeline_wind/model
INI=/e/qp.test/pipeline_wind/ini/job.update.ini

# TradeDate
echo '* TradeDate'
cd $MAIN_DIR/TradeDate
python TradeDate.py $INI

# estu.a 
echo '* estu.a'
cd $MAIN_DIR/estu.a
python estu.a.py $INI

# stop
echo '* stop'
cd $MAIN_DIR/stop
python stop.py $INI

# estu.r 
echo '* estu.r'
cd $MAIN_DIR/estu.r
python estu.r.py $INI

#shares
echo '* shares'
cd $MAIN_DIR/shares
python shares.py $INI

#split
echo '* split'
cd $MAIN_DIR/split
python split.py $INI

#snew
echo '* snew'
cd $MAIN_DIR/snew
python snew.py $INI

#st
echo '* st'
cd $MAIN_DIR/st
python st.py $INI

#dividend:dividend after tax
echo '* dividend'
cd $MAIN_DIR/dividend
python dividend.py $INI

#dividend2:dividend before tax
echo '* dividend2'
cd $MAIN_DIR/dividend2
python dividend.py $INI

#delisted
echo '* delisted'
cd $MAIN_DIR/delisted
python delisted.py $INI

##EODPrice
#echo '* EODPrice'
#cd $MAIN_DIR/EODPrice
#python EODPrice.py $INI

#PreAdjPrice
echo '* PreAdjPrice'
cd $MAIN_DIR/PreAdjPrice
python PreAdjPrice.py $INI morning

#EODIndex
echo '* EODIndex'
cd $MAIN_DIR/EODIndex
python EODIndex.py $INI

#index
echo '* index'
cd $MAIN_DIR/index
python Index.py $INI

#ConWeight
echo '* ConWeight'
cd $MAIN_DIR/ConWeight
python ConWeight.py $INI

#Other
echo '* other'
cd $MAIN_DIR/other
python other.py $INI

#indzx
echo '* indzx'
cd $MAIN_DIR/indzx
python indzx.py $INI

echo '----------------- [Morning] Basic Pipeline Ends -----------------'
