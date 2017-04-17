#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from biocluster.config import Config
import socket
import os

config = Config()


def check_port_working():

    try:
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.connect(config.wpm_listen)
        ss.shutdown(2)
        ss.close()
    except socket.error:
        return False

    try:
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.connect(config.wpm_logger_listen)
        ss.shutdown(2)
        ss.close()
    except socket.error:
        return False

    return True


def stop_old():
    main_pid_file = config.wpm_pid_dir + "/wpm.pid"
    process_pid_file = config.wpm_pid_dir + "/pm.pid"
    log_pid_file = config.wpm_pid_dir + "/lm.pid"
    if os.path.exists(main_pid_file):
        os.system("/bin/kill -9 `cat %s`" % main_pid_file)
        os.remove(main_pid_file)
    if os.path.exists(process_pid_file):
        os.system("/bin/kill -9 `cat %s`" % process_pid_file)
        os.remove(process_pid_file)
    if os.path.exists(log_pid_file):
        os.system("/bin/kill -9 `cat %s`" % log_pid_file)
        os.remove(log_pid_file)

if __name__ == '__main__':
    if not check_port_working():
        stop_old()
        os.system("/sbin/service bcl-wpm restart")
