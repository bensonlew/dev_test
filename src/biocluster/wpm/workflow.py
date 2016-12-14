# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from multiprocessing import Process, Queue
from ..core.function import load_class_by_path, hostname
import os
import json
import traceback
import datetime
from ..config import Config
import time
import sys
import setproctitle
from .logger import Logger
from .client import worker_client, log_client


class WorkflowWorker(Process):
    """
    流程新建进程
    """
    def __init__(self, wsheet, **kwargs):
        """

        :param wsheet:  运行流程所需的Sheet对象
        :param log_queue:  multiprocessing.Queue 对象，用于将运行日志信息传导到管理进程
        :param action_queue:  multiprocessing.Queue 对象，用于从管理进程中传递暂停，退出等信号到流程进程
        """
        super(WorkflowWorker, self).__init__(**kwargs)
        self.wsheet = wsheet
        self.action_queue = Queue()
        self.config = Config()
        self.logger = Logger(log_type="Worker[%s]" % wsheet.id)

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
                path = self.wsheet.type
            else:
                path = "single"
            workflow_module = load_class_by_path(path, "Workflow")
            workflow = workflow_module(self.wsheet)
            workflow.action_queue = self.action_queue
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
                stage_id = 0
                if "stage_id" in json_data.keys():
                    stage_id = json_data["stage_id"]
                json_obj = {"stage": {
                    "task_id": json_data["id"],
                    "stage_id": stage_id,
                    "created_ts": datetime.datetime.now(),
                    "error": error,
                    "status": "failed",
                    "run_time": 0}}
                post_data = {
                    "content": json_obj
                }
                data = {
                    "task_id": json_data["id"],
                    "api": json_data["UPDATE_STATUS_API"],
                    "data": json.dumps(post_data)
                }

                log_client().add_log(data)
            worker_client().set_error(json_data["id"], error)
            self.logger.info("运行异常: %s " % error)
