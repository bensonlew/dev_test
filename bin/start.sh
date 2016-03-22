#!/bin/bash
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
cd ~/biocluster/bin/
./run_workflow.py -b -s
./api_update.py -m server
./upload_result.py