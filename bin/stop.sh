#!/bin/bash
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
cd ~/biocluster/bin/
PID_FILE=`echo ../run/$HOSTNAME.pid`
kill -s SIGTERM `cat $PID_FILE`
#rm $PID_FILE
PID_FILE=`echo ../run/$HOSTNAME.api.pid`
kill -s SIGTERM `cat $PID_FILE`
#rm $PID_FILE
PID_FILE=`echo ../run/$HOSTNAME.upload.pid`
kill -s SIGTERM `cat $PID_FILE`
#rm $PID_FILE