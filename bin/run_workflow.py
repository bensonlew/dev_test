#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import argparse


def main():
    parser = argparse.ArgumentParser(description="run a workflow")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-d", "--database", action="store_true", help="read json from database,config by main.conf file")
    group.add_argument("-j", "--json", help="read json from a json file")
    group.add_argument("-r", "--rerun_id", help="input a workflow id in database and rerun it.\n"
                                                "Note:Will remove the workspace file last run")
    parser.add_argument("-c", "--record", choices=["yes", "no"], default="yes", help="record run logs in database")
    parser.add_argument("-b", "--daemon", action="store_true",  help="run a workflow in daemon background mode")
    parser.parse_args()

    # print parser.print_help()

def read_from_database():
    pass


if __name__ == "__main__":
    main()
