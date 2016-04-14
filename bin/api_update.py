#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from gevent import monkey; monkey.patch_all()
import argparse
from biocluster.api.web.log import LogManager
from biocluster.config import Config
import os
from biocluster.core.function import hostname, daemonize
import atexit
import datetime

parser = argparse.ArgumentParser(description="update date to remote api")
parser.add_argument("-m", "--mode", choices=["server", "retry"], default="retry", help="run mode")
parser.add_argument("-a", "--api", help="only for retry mode, the api type to retry, must be given!")

args = parser.parse_args()


def delpid():
    pid_file = Config().SERVICE_PID
    pid_file = pid_file.replace('$HOSTNAME', hostname + ".api")
    os.remove(pid_file)
    print("%s\t用户终止监控，终止进程,  pid: %s " % (datetime.datetime.now(), os.getpid()))


def writepid():
    pid = str(os.getpid())
    pid_file = Config().SERVICE_PID
    pid_file = pid_file.replace('$HOSTNAME', hostname + ".api")
    if not os.path.exists(os.path.dirname(pid_file)):
        os.mkdir(os.path.dirname(pid_file))
    with open(pid_file, 'w+') as f:
        f.write('%s\n' % pid)
    atexit.register(delpid)


def main():
    lm = LogManager()
    if args.mode == "retry":
        if args.api:
            lm.api = args.api
        lm.update()
    else:
        pid_file = Config().SERVICE_PID
        pid_file = pid_file.replace('$HOSTNAME', hostname + ".api")
        if os.path.isfile(pid_file):
            raise Exception("PID file already exists,if this service already running?")
        daemonize()
        writepid()
        lm.update_as_service()

if __name__ == "__main__":
    main()
