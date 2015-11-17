#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import argparse
import json
import sys
import datetime
import os
import time
from biocluster.core.function import load_class_by_path, daemonize
from biocluster.wsheet import Sheet
from biocluster.config import Config
from multiprocessing import Process
import atexit

parser = argparse.ArgumentParser(description="run a workflow")
group = parser.add_mutually_exclusive_group()
group.add_argument("-s", "--service", action="store_true", help="run in service mode,"
                                                                "read json from database,config by main.conf file")
group.add_argument("-j", "--json", help="read json from a json file")
group.add_argument("-r", "--rerun_id", help="input a workflow id in database and rerun it.\n"
                                            "Note:Will remove the workspace file last run")
# parser.add_argument("-c", "--record", choices=["Y", "N"], default="Y", help="record run logs in database")
parser.add_argument("-b", "--daemon", action="store_true", help="run a workflow in daemon background mode")
logfile = os.getcwd() + "/log.txt"
parser.add_argument("-l", "--log", default=logfile,  help="write a log file,lose efficacy when use service mode,"
                                                          "in service mode,use the config in main.conf file!")
args = parser.parse_args()


def main():
    if not (args.service or args.json or args.rerun_id):
        parser.print_help()
        sys.exit(1)
    wj = WorkJob()
    if args.service:
        if os.path.isfile(Config().SERVICE_PID):
            raise Exception("PID file already exists,if this service already running?")
        if args.daemon:
            daemonize(stderr=Config().SERVICE_LOG, stdout=Config().SERVICE_LOG)

        write_log("start running in service mode ...")
        writepid()
        process_array = []
        while True:
            time.sleep(Config().SERVICE_LOOP)
            json_data = check_run()
            if json_data:
                process = Process(target=wj.start, args=(json_data,))
                process_array.append(process)
                process.start()
            for p in process_array:
                if not p.is_alive():
                    p.join()
                    process_array.remove(p)
    else:
        if args.daemon:
            daemonize(stderr=args.log, stdout=args.log)
        json_data = check_run()
        if json_data:
            wj.start(json_data)


def check_run():
    wj = WorkJob()
    wj.lock()
    json_data = None
    if args.service:
        json_data = wj.get_from_database()
    elif args.json:
        json_data = wj.get_from_file(args.json)
    elif args.rerun_id:
        json_data = wj.get_from_database(args.rerun_id)
    if json_data:
        wj.update_workflow()
    wj.unlock()
    return json_data


def delpid():
    os.remove(Config().SERVICE_PID)


def writepid():
    pid = str(os.getpid())
    with open(Config().SERVICE_PID, 'w+') as f:
        f.write('%s\n' % pid)
    atexit.register(delpid)


def write_log(data):
    log_file = args.log
    if args.service and args.daemon:
        log_file = Config().SERVICE_LOG
    with open(log_file, "a") as f:
        line = str(datetime.datetime.now()) + "\t" + data
        f.write(line + "\n")


class WorkJob(object):
    def __init__(self):
        self.workflow_id = args.rerun_id
        self.pid = os.getpid()
        self._db = Config().get_db()

    @property
    def db(self):
        if self.pid != os.getpid():
            self._db = Config().get_db()
        return self._db

    def lock(self):
        self.db.query("LOCK TABLE `workflow` WRITE")

    def get_from_database(self, workflow_id=None):
        if workflow_id:
            results = self.db.query("SELECT * FROM workflow WHERE workflow_id=$workflow_id",
                                    vars={'workflow_id': workflow_id})
        else:
            results = self.db.query("SELECT * FROM workflow WHERE has_run = 0 order by id desc limit 0,1")
        if len(results) > 0:
            data = results[0]
            self.workflow_id = data.workflow_id
            return json.loads(data.json)

    def unlock(self):
        self.db.query("UNLOCK TABLES")

    def get_from_file(self, path):
        with open(path, 'r') as f:
            data = json.load(f)
            if "type" not in data.keys() or "id" not in data.keys():
                raise Exception("Json格式错误")
            self.workflow_id = data["id"]
            self.insert_workflow(data)
            return data

    def insert_workflow(self, data):
        results = self.db.query("SELECT * FROM workflow WHERE workflow_id=$workflow_id",
                                vars={'workflow_id': data["id"]})
        if len(results) < 1:
            return self.db.insert("workflow", workflow_id=data["id"], json=json.dumps(data))
        else:
            # write_log("Workflow %s already in database, skip insert ..." % data["id"])
            raise Exception("Workflow %s already in database, Please use -r rerun it!")

    def update_workflow(self):
        data = {
            "server": hostname(),
            "has_run": 1,
            "run_time": datetime.datetime.now(),
            "pid": os.getpid(),
            "is_end": 0,
            "is_error": 0,
            "error": ""
        }
        self.db.update("workflow", where="workflow_id = %s" % self.workflow_id, **data)

    # def run(self):
    #     self.lock()
    #     self.get_json()
    #
    #     self.update_workflow()
    #     self.unlock()
    #     self.start()

    def start(self, json_data):
        self.workflow_id = json_data["id"]
        write_log("Start running workflow:%s" % self.workflow_id)
        if json_data["type"] == "workflow":
            path = json_data["name"]
        elif json_data["type"] == "link":
            path = "link"
        else:
            path = "single"

        try:
            wf = load_class_by_path(path, "Workflow")
            wsheet = Sheet(data=json_data)
            workflow = wf(wsheet)
            workflow.config.USE_DB = True
            workflow.run()
        except Exception, e:
            data = {
                "is_error": 1,
                "error": "运行异常:%s: %s" % (e.__class__.__name__, e),
                "end_time": datetime.datetime.now(),
                "is_end": 1
            }
            self.db.update("workflow", where="workflow_id = %s" % self.workflow_id, **data)

        write_log("End running workflow:%s" % self.workflow_id)


def hostname():
    sys_name = os.name
    if sys_name == 'nt':
            host_name = os.getenv('computername')
            return host_name
    elif sys_name == 'posix':
            with os.popen('echo $HOSTNAME') as f:
                host_name = f.readline()
                host_name.strip("\n")
                return host_name
    else:
            return 'Unkwon hostname'

if __name__ == "__main__":
    main()
