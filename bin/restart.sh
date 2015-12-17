#!/bin/bash
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
kill -9 `cat ../run.pid`
rm ../run.pid
./run_workflow.py -b -s
