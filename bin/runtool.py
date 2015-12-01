#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import argparse
import pickle
from biocluster.agent import PickleConfig
import os
from biocluster.core.function import load_class_by_path, daemonize
parser = argparse.ArgumentParser(description="run a workflow")
parser.add_argument("-b", "--daemon", action="store_true", help="run in daemon background mode")
parser.add_argument("-d", "--debug", action="store_true", help="run in debug mode,will not use network!")
parser.add_argument("tool", type=str,
                    help="the tool name to run")
args = parser.parse_args()


def main():
    name = args.tool
    class_file = name + "_class.pk"
    if args.daemon:
        daemonize(stdout="%s.o" % name, stderr="%s.e" % name)
        write_pid()
    with open(class_file, "r") as f:
        class_list = pickle.load(f)
    for file_class in class_list['files']:
        load_class_by_path(file_class, "File")
    # print class_list['tool']
    paths = class_list['tool'].split(".")
    paths.pop(0)
    paths.pop(0)
    tool = load_class_by_path(".".join(paths), "Tool")
    config_file = name + ".pk"
    with open(config_file, "r") as f:
        config = pickle.load(f)
    # print vars(config)
    if args.debug:
        config.DEBUG = True
    else:
        config.DEBUG = False
    tool(config).run()


def write_pid():
    with open("run.pid", "w") as f:
        f.write("%s" % os.getpid())

if __name__ == '__main__':
    main()


