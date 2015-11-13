#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import argparse
import json
import sys
from biocluster.config import Config
import datetime
import os
import socket
import time
from biocluster.core.function import load_class_by_path
from biocluster.wsheet import Sheet

parser = argparse.ArgumentParser(description="run a workflow")
group = parser.add_mutually_exclusive_group()
group.add_argument("-d", "--database", action="store_true", help="read json from database,config by main.conf file")
group.add_argument("-j", "--json", help="read json from a json file")
group.add_argument("-r", "--rerun_id", help="input a workflow id in database and rerun it.\n"
                                            "Note:Will remove the workspace file last run")
# parser.add_argument("-c", "--record", choices=["Y", "N"], default="Y", help="record run logs in database")
parser.add_argument("-b", "--daemon", action="store_true", help="run a workflow in daemon background mode")
logfile = os.getcwd() + "/log.txt"
parser.add_argument("-l", "--log", default=logfile,  help="write a log file")
args = parser.parse_args()

DB = Config().get_db()


def main():
    if not (args.database or args.json or args.rerun_id):
        parser.print_help()
        sys.exit(1)
    if args.database:
        args.database = True
        write_log("Reading data from database,loop checking ...")
        while True:
            wj = WorkJob()
            wj.run()
            time.sleep(2)


def write_log(data):
    with open(args.log, "a") as f:
        line = str(datetime.datetime.now()) + "\t" + data + "\n"
        f.write(line)


class WorkJob(object):
    def __init__(self):
        self.db = DB
        self.database_id = None
        self.workflow_id = args.rerun_id
        self.json_data = None
        self.pid = None

    def get_json(self):
        if args.database:
            self.json_data = self.get_from_database()
        elif args.json:
            self.json_data = self.get_from_file(args.json)
            self.database_id = self.insert_workflow(self.json_data)
        elif args.rerun_id:
            self.json_data = self.get_from_database(args.rerun_id)

    def lock(self):
        self.db.query("LOCK TABLE `workflow` WRITE")

    def get_from_database(self, workflow_id=None):
        if workflow_id:
            results = self.db.query("SELECT * FROM workflow WHERE workflow_id=$workflow_id",
                                    vars={'workflow_id': self.workflow_id})
        else:
            results = self.db.query("SELECT * FROM workflow WHERE has_run = 0 order by id desc limit 0,1")
        if len(results) > 0:
            data = results[0]
            self.database_id = data.id
            self.workflow_id = data.workflow_id
            return json.loads(data.json)
        else:
            # write_log("No workflow is ready to run in the database!")
            sys.exit(0)

    def unlock(self):
        self.db.query("UNLOCK TABLES")

    def get_from_file(self, path):
        with open(path, 'r') as f:
            data = json.load(f)
            self.workflow_id = data.workflow_id
            return data

    def insert_workflow(self, data):
        results = self.db.query("SELECT * FROM workflow WHERE workflow_id=$workflow_id",
                                vars={'workflow_id': data.workflow_id})
        if len(results) < 1:
            return self.db.insert("workflow", workflow_id=data.workflow_id, json=json.dumps(data))
        else:
            write_log("Workflow %s already in database, skip insert ..." % data.workflow_id)
            return results[0].id

    def update_workflow(self, pid):
        data = {
            "server": socket.gethostname(),
            "has_run": 1,
            "run_time": datetime.datetime.now(),
            "pid": pid,
            "is_end": 0,
            "is_error": 0,
            "error": ""
        }
        self.db.update("workflow", where="id = %s" % self.database_id, **data)

    def daemonize(self, stdin='/dev/null', stdout='/dev/null', stderr='dev/null'):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
            sys.exit(1)

        # 从母体环境脱离
        os.chdir("/")
        os.umask(0)
        os.setsid()
        # 执行第二次fork
        try:
            self.pid = os.fork()
            if self.pid > 0:
                sys.exit(0)  # second parent out
        except OSError, e:
            sys.stderr.write("fork #2 failed: (%d) %s]n" % (e.errno, e.strerror))
            sys.exit(1)

        # 进程已经是守护进程了，重定向标准文件描述符
        for f in sys.stdout, sys.stderr:
            f.flush()
        si = file(stdin, 'r')
        so = file(stdout, 'a+')
        se = file(stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

    def run(self):
        self.lock()
        self.get_json()
        if args.daemon:
            self.daemonize(stderr=args.log)
        else:
            self.pid = os.getpid()
        self.update_workflow(self.pid)
        self.unlock()
        self.start()

    def start(self):
        write_log("Start running workflow:%s" % self.workflow_id)
        if self.json_data["type"] == "workflow":
            path = self.json_data["name"]
        elif self.json_data["type"] == "link":
            path = "link"
        else:
            path = "single"
        wf = load_class_by_path(path, "workflow")
        wsheet = Sheet(data=self.json_data)

        try:
            workflow = wf(wsheet)
            workflow.config.USE_DB = True
            workflow.run()
        except Exception, e:
            data = {
                "is_error": 1,
                "error": "%s: %s" % (e.__class__.__name__, e),
                "end_time": datetime.datetime.now(),
                "is_end": 1
            }
            self.db.update("workflow", where="id = %s" % self.database_id, **data)

        write_log("End running workflow:%s" % self.workflow_id)


if __name__ == "__main__":
    main()
