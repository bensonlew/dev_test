#!/bin/bash

su -l sanger -c "cd ~/biocluster/bin/;./run_workflow.py -s -b;./api_update.py -m server"
