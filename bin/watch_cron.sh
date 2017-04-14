#!/bin/bash
#
PYTHON_PATH=/mnt/ilustre/users/sanger-dev/app/program/Python
BCL_PATH=/mnt/ilustre/users/sanger-dev/biocluster

export PATH=$PYTHON_PATH/bin:$PATH
export PYTHONPATH=$BCL_PATH/src:$PYTHONPATH

python $BCL_PATH/bin/watch_wpm.py
