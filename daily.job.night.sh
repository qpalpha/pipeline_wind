#!/usr/bin/bash
#-*- coding:utf-8 -*-
####################################
#File Name:     daily.job.night1.sh
#Author:        qp
#Mail:          
#Created Time:  2019-06-14 09:02:40
####################################

echo '----------------- Basic Pipeline Starts -----------------'

MAIN_DIR=/e/qp.test/pipeline_wind/model

# TradeDate
echo '* TradeDate'
cd $MAIN_DIR/TradeDate
python TradeDate.py  TradeDate.ini

# estu.a 
echo '* estu.a'
cd $MAIN_DIR/estu.a
python estu.a.py estu.a.ini

# stop
echo '* stop'
cd $MAIN_DIR/stop
python stop.py stop.ini

# estu.r 
echo '* estu.r'
cd $MAIN_DIR/estu.r
python estu.r.py estu.r.ini

#share
echo '* shares'
cd $MAIN_DIR/shares
python shares.py shares.ini

#split
echo '* split'
cd $MAIN_DIR/split
python split.py split.ini

#snew
echo '* snew'
cd $MAIN_DIR/snew
python snew.py snew.ini

#st
echo '* st'
cd $MAIN_DIR/st
python st.py st.ini

#dividend
echo '* dividend'
cd $MAIN_DIR/dividend
python dividend.py dividend.ini

#delisted
echo '* delisted'
cd $MAIN_DIR/delisted
python delisted.py delisted.ini

#EODPrice
echo '* EODPrice'
cd $MAIN_DIR/EODPrice
#python EODPrice.py EODPrice.ini

#EODIndex
echo '* EODIndex'
cd $MAIN_DIR/EODIndex
#python EODIndex.py EODIndex.ini

#index
echo '* index'
cd $MAIN_DIR/index
python Index.py csi300.ini
python Index.py csi500.ini

#ConWeight
echo '* ConWeight'
cd $MAIN_DIR/ConWeight
python ConWeight.py csi300_wt.ini
python ConWeight.py csi500_wt.ini


echo '----------------- Basic Pipeline Ends -----------------'
