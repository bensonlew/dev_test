# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from multiprocessing import Process
from ..core.function import load_class_by_path, hostname, add_log_queue, add_run_queue
import os
import json
import traceback
import datetime
from ..config import Config
import time
import sys
import setproctitle
from .logger import Logger
# from .client import worker_client, log_client


class WorkflowWorker(Process):
    """
    流程新建进程
    """
    def __init__(self, wsheet, action_queue, run_info_queue, log_info_queue, rpc_message_queue,
                 rpc_callback_queue, rpc_restart_signal):
        """

        :param wsheet:  运行流程所需的Sheet对象
        :param log_queue:
        :param action_queue:
        """
        super(WorkflowWorker, self).__init__()
        self.wsheet = wsheet
        self.wsheet.action_queue = action_queue
        self.wsheet.run_info_queue = run_info_queue
        self.wsheet.log_info_queue = log_info_queue
        self.config = Config()
        self.logger = Logger(log_type="Worker[%s]" % wsheet.id)
        self.wsheet.rpc_message_queue = rpc_message_queue
        self.wsheet.rpc_callback_queue = rpc_callback_queue
        self.wsheet.rpc_restart_signal = rpc_restart_signal

    def run(self):
        super(WorkflowWorker, self).run()
        setproctitle.setproctitle("WPM[worker %s]" % self.wsheet.id)
        # 输出重定向
        timestr = time.strftime('%Y%m%d', time.localtime(time.time()))
        log_dir = os.path.join(self.config.wpm_log_file, timestr)
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        log = os.path.join(log_dir, "%s_%s.log" % (self.wsheet.id, hostname))
        so = file(log, 'a+')
        se = file(log, 'a+', 0)
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        try:
            if self.wsheet.type == "workflow":
                path = self.wsheet.name
            else:
                path = "single"

            workflow_module = load_class_by_path(path, "Workflow")
            workflow = workflow_module(self.wsheet)

            file_path = os.path.join(workflow.work_dir, "data.json")
            with open(file_path, "w") as f:
                json.dump(self.wsheet.data, f, indent=4)
            workflow.run()
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            error = "运行异常:%s: %s" % (e.__class__.__name__, e)
            json_data = self.wsheet.data
            if json_data and "UPDATE_STATUS_API" in json_data.keys():
                json_obj = {"task": {
                    "task_id": json_data["id"],
                    "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "failed",
                    "run_time": 0,
                    "progress": 0
                    },
                    "log": [
                        {
                            "status": "failed",
                            "run_time": 0,
                            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "desc": "运行异常: %s" % e
                        }
                    ]
                }
                post_data = {
                    "sync_task_log": json_obj
                }
                data = {
                    "task_id": json_data["id"],
                    "api": json_data["UPDATE_STATUS_API"],
                    "data": post_data
                }
                if "update_info" in self.wsheet.options().keys():
                    data["update_info"] = self.wsheet.option('update_info')
                # log_client().add_log(data)
                # self.log_info_queue.put(("add_log", (data, )))
                add_log_queue(self.wsheet.log_info_queue, "add_log", data)
            # worker_client().set_error(json_data["id"], error)
            # self.run_info_queue.put(("set_error", (json_data["id"], error)))
            add_run_queue(self.wsheet.run_info_queue, "set_error", json_data["id"], error)
            self.logger.info("运行异常: %s " % error)


