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

parser = argparse.ArgumentParser(description="start biocluster worker manager service")
parser.add_argument("-s", "--server", action="store_true", help="use the daemon mode to ran server")
args = parser.parse_args()


def check_port():
    config = Config()
    try:
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.connect(config.wpm_listen)
        ss.shutdown(2)
        ss.close()
        print "端口被占用:%s%s" % (config.wpm_listen[0], config.wpm_listen[1])
    except Exception:
        return True
    try:
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.connect(config.wpm_logger_listen)
        ss.shutdown(2)
        ss.close()
        print "端口被占用:%s%s" % (config.wpm_logger_listen[0], config.wpm_logger_listen[1])
    except Exception:
        return True
    return False

if not check_port():
    sys.exit(1)
else:
    if args.server:
        daemonize()

    server = MainServer()


    def writepid():
        pid = str(os.getpid())
        pid_file = "/var/run/bcl-wpm/wpm.pid"
        if not os.path.exists(os.path.dirname(pid_file)):
            os.mkdir(os.path.dirname(pid_file))
        with open(pid_file, 'w+') as f:
            f.write('%s\n' % pid)


    def kill_sub_service(signum, frame):
        write_log("关闭进程管理器...")
        os.system("kill -9 %s" % server.manager_server.pid)
        write_log("关闭API LOG监听...")
        os.system("kill -9 %s" % server.api_log_server.pid)
        write_log("关闭WPM主服务监听...")

    signal.signal(signal.SIGTERM, kill_sub_service)
    signal.signal(signal.SIGINT, kill_sub_service)
    writepid()
    server.start()

