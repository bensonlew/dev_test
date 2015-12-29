#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from gevent import monkey; monkey.patch_socket()
import argparse
from biocluster.api.web.log import LogManager
parser = argparse.ArgumentParser(description="update date to remote api")
parser.add_argument("-m", "--mode", choices=["server", "retry"], default="retry", help="run mode")
parser.add_argument("-a", "--api", help="only for retry mode, the api type to retry, must be given!")

args = parser.parse_args()


def main():
    lm = LogManager()
    if args.mode == "retry":
        if args.api:
            lm.api = args.api
        lm.update()
    else:
        lm.update_as_service()

if __name__ == "__main__":
    main()
