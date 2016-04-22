#!/bin/bash
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
cd ~/biocluster/bin/
PID_FILE=`echo ../run/$HOSTNAME.pid`
if [ -f $PID_FILE ]
then
    kill `cat $PID_FILE`
fi
#rm $PID_FILE

PID_FILE=`echo ../run/$HOSTNAME.api.pid`
if [ -f $PID_FILE ]
then
    kill `cat $PID_FILE`
fi
#rm $PID_FILE

PID_FILE=`echo ../run/$HOSTNAME.upload.pid`
if [ -f $PID_FILE ]
then
    kill `cat $PID_FILE`
fi
#rm $PID_FILE

while true
do
    if [ "`ls -A ../run/`" = "" ]; then
        ./run_workflow.py -b -s
        ./api_update.py -m server
        ./upload_result.py -s
        break
    else
        sleep 1
    fi
done

su -l root -c "service httpd restart"
