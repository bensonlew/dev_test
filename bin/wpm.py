#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from biocluster.core.function import daemonize, hostname
from biocluster.wpm.listener import MainServer, write_log
import signal
import argparse
import os
from biocluster.config import Config
import socket
import sys

config = Config()

parser = argparse.ArgumentParser(description="start biocluster worker manager service")
parser.add_argument("-s", "--server", action="store_true", help="use the daemon mode to ran server")
args = parser.parse_args()


def check_port():

    try:
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.connect(config.wpm_listen)
        ss.shutdown(2)
        ss.close()
        print "端口被占用:%s%s" % (config.wpm_listen[0], config.wpm_listen[1])
    except socket.error:
        return True
    try:
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.connect(config.wpm_logger_listen)
        ss.shutdown(2)
        ss.close()
        print "端口被占用:%s%s" % (config.wpm_logger_listen[0], config.wpm_logger_listen[1])
    except socket.error:
        return True
    return False

if not check_port():
    sys.exit(1)
else:
    if args.server:
        daemonize()

    server = MainServer()
    server.start()
