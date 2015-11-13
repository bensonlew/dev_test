#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'


import pickle
from biocluster.agent import PickleConfig
import importlib
import sys
from biocluster.core.function import load_class_by_path


def main():
    if len(sys.argv) < 2:
        raise Exception("必须输入参数!")
    name = sys.argv[1]
    class_file = name + "_class.pk"
    with open(class_file, "r") as f:
        class_list = pickle.load(f)
    for file_class in class_list['files']:
        load_class_by_path(file_class, "File")
    #print class_list['tool']
    paths= class_list['tool'].split(".")
    paths.pop(0)
    paths.pop(0)
    tool = load_class_by_path(".".join(paths), "Tool")
    config_file = name + ".pk"
    with open(config_file, "r") as f:
        config = pickle.load(f)
    #print vars(config)
    tool(config).run()

if __name__ == '__main__':
    main()


