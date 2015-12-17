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
import traceback
import web

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
    if args.service:
        if os.path.isfile(Config().SERVICE_PID):
            raise Exception("PID file already exists,if this service already running?")
        date_run = ""
        if args.daemon:
            log = getlogpath()
            daemonize(stderr=log, stdout=log)
            date_run = time.strftime('%Y%m%d', time.localtime(time.time()))
        write_log("start running in service mode ...")
        writepid()
        process_array = []
        wj = WorkJob()
        while True:
            if date_run != time.strftime('%Y%m%d', time.localtime(time.time())):
                date_run = time.strftime('%Y%m%d', time.localtime(time.time()))
                log = getlogpath()
                so = file(log, 'a+')
                se = file(log, 'a+', 0)
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())
            time.sleep(Config().SERVICE_LOOP)
            t = wj.db.transaction()
            try:
                json_data = check_run(wj)
            except Exception, e:
                exstr = traceback.format_exc()
                print exstr
                write_log("运行出错: %s" % e)
                t.rollback()
                continue
            else:
                t.commit()
            if json_data:
                # process = Process(target=wj.start, args=(json_data,))
                process = Worker(wj, json_data)
                process_array.append(process)
                process.start()
                write_log("Running workflow %s,the process id %s ..." % (json_data["id"], process.pid))
                while len(process_array) >= Config().SERVICE_PROCESSES:
                    write_log("Running workflow %s, reach the max limit,waiting ..." % json_data["id"])

                    for p in process_array:
                        if not p.is_alive():
                            exitcode = p.exitcode
                            if exitcode != 0:
                                write_log("流程%s运行出错: 程序运行异常" % p.json_data["id"])
                                p.wj.update_error()
                            p.join()
                            process_array.remove(p)
                    time.sleep(1)
            for p in process_array:
                if not p.is_alive():
                    exitcode = p.exitcode
                    if exitcode != 0:
                        write_log("流程%s运行出错: 程序运行异常" % p.json_data["id"])
                        p.wj.update_error()
                    p.join()
                    process_array.remove(p)
    else:
        if args.daemon:
            daemonize(stderr=args.log, stdout=args.log)
        wj = WorkJob()
        json_data = check_run(wj)
        if json_data:
            wj.start(json_data)


def check_run(wj):
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


def getlogpath():
    timestr = time.strftime('%Y%m%d', time.localtime(time.time()))
    if not os.path.exists(Config().SERVICE_LOG):
        os.mkdir(Config().SERVICE_LOG)
    log = os.path.join(Config().SERVICE_LOG, "%s.log" % timestr)
    return log


def write_log(data):
    log_file = args.log
    if args.service and args.daemon:
        log_file = getlogpath()
    with open(log_file, "a") as f:
        line = str(datetime.datetime.now()) + "\t" + data
        f.write(line + "\n")


class Worker(Process):
    def __init__(self, wj, json_data):
        super(Worker, self).__init__()
        self.wj = wj
        self.json_data = json_data

    def run(self):
        super(Worker, self).run()
        timestr = time.strftime('%Y%m%d', time.localtime(time.time()))
        log_dir = os.path.join(Config().SERVICE_LOG, timestr)
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        log = os.path.join(log_dir, "%s.log" % self.json_data["id"])
        so = file(log, 'a+')
        se = file(log, 'a+', 0)
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        self.wj.start(self.json_data)


class WorkJob(object):
    def __init__(self):
        self.workflow_id = args.rerun_id
        self.pid = os.getpid()
        self._db = None
        self.client = None

    @property
    def db(self):
        if self.pid != os.getpid():
            self.pid = os.getpid()
            self._db = Config().get_db()
        if not self._db:
            self._db = Config().get_db()
        return self._db

    def get_client_limit(self):
        if self.client:
            clients = self.db.query("SELECT * FROM clientkey where client=$client", vars={'client': self.client})
            if len(clients) > 0:
                client = clients[0]
                if client.max_workflow != "" and client.max_workflow is not None:
                    return client.max_workflow
        return 0

    def get_running_workflow(self):
        where_dict = dict(client=self.client, has_run=1, is_end=0, is_error=0)
        return len(self.db.select("workflow", where=web.db.sqlwhere(where_dict)))

    def lock(self):
        self.db.query("LOCK TABLE `workflow` WRITE")

    def get_from_database(self, workflow_id=None):
        if workflow_id:
            results = self.db.query("SELECT * FROM workflow WHERE  workflow_id=$workflow_id",
                                    vars={'workflow_id': workflow_id})
        else:
            results = self.db.query("SELECT * FROM workflow WHERE has_run = 0 order by id desc limit 0,1")
        if len(results) > 0:
            data = results[0]
            self.workflow_id = data.workflow_id
            self.client = data.client
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
        myvar = dict(id=self.workflow_id)
        self.db.update("workflow", vars=myvar, where="workflow_id = $id", **data)

    # def run(self):
    #     self.lock()
    #     self.get_json()
    #
    #     self.update_workflow()
    #     self.unlock()
    #     self.start()

    def start(self, json_data):
        if self.client:
            max_limit = self.get_client_limit()
            if max_limit > 0:
                while True:
                    running = self.get_running_workflow()
                    if running <= max_limit:
                        break
                    else:
                        write_log("running workflow %s reach the max limit of client %s ,waiting for 1 minute "
                                  "and check again ..." % (self.workflow_id, self.client))
                        time.sleep(60)

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
            file_path = os.path.join(workflow.work_dir, "data.json")
            with open(file_path, "w") as f:
                json.dump(json_data, f, indent=4)
            workflow.run()
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            write_log("Workflow %s has error %s:%s" % (self.workflow_id, e.__class__.__name__, e))
            data = {
                "is_error": 1,
                "error": "运行异常:%s: %s" % (e.__class__.__name__, e),
                "end_time": datetime.datetime.now(),
                "is_end": 1
            }
            myvar = dict(id=self.workflow_id)
            self.db.update("workflow", vars=myvar, where="workflow_id = $id", **data)

        write_log("End running workflow:%s" % self.workflow_id)

    def update_error(self):
        myvar = dict(id=self.workflow_id)
        results = self.db.query("SELECT * FROM workflow WHERE workflow_id=$id and is_end=0 and is_error=0",
                                vars=myvar)
        if len(results) > 0:
            self.db.update("workflow", vars=myvar, where="workflow_id = $id", is_error=1, error="程序运行异常")


def hostname():
    sys_name = os.name
    if sys_name == 'nt':
            host_name = os.getenv('computername')
            return host_name
    elif sys_name == 'posix':
            with os.popen('echo $HOSTNAME') as f:
                host_name = f.readline()
                return host_name.strip('\n')
    else:
            return 'Unkwon hostname'

if __name__ == "__main__":
    main()
