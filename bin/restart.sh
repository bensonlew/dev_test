#!/bin/bash
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
cd ~/biocluster/bin/
PID_FILE=`echo ../run/$HOSTNAME.pid`
kill -9 `cat $PID_FILE`
rm $PID_FILE
./run_workflow.py -b -s
PID_FILE=`echo ../run/$HOSTNAME.api.pid`
kill -9 `cat $PID_FILE`
rm $PID_FILE
./api_update.py -m server
su -l root -c "service httpd restart"
