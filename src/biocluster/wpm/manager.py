# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from ..core.singleton import singleton
from multiprocessing import Event, Queue, Process
from .workflow import WorkflowWorker
from ..wsheet import Sheet
from .logger import Logger
from .log import LogWorker
from ..config import Config
from .db import WorkflowModel, CheckModel
import os
import traceback
from pwd import getpwnam
from multiprocessing.managers import BaseManager
from threading import Thread
import setproctitle
import time
import gevent
import sys
from ..core.function import hostname


@singleton
class WorkflowManager(object):
    def __init__(self):
        self.queue = Queue()
        self.workflows = {}
        self.event = {}
        self.return_msg = {}
        self.logger = Logger()
        self.server = None

    def add_task(self, json):
        if not isinstance(json, dict) or "id" not in json.keys():
            self.logger.error("add workflow %s format error!" % json)
            return {"success": False, "info": "json格式错误"}
        json["WPM"] = True
        wsheet = Sheet(data=json)
        model = WorkflowModel(wsheet)
        if wsheet.id in self.workflows.keys():
            self.logger.error("Workflow %s has already run! " % wsheet.id)
            return {"success": False, "info": "Workflow %s正在运行，不能重复添加!" % wsheet.id}
        if model.find():
            self.logger.error("workflow %s is already exists!" % wsheet.id)
            return {"success": False, "info": "Workflow %s已经存在，不能重复运行!" % wsheet.id}
        self.workflows[wsheet.id] = model
        model.save()
        self.queue.put(json)
        self.logger.info("接收到Workflow[%s] 请求,放入队列..." % wsheet.id)
        return {"success": True, "info": ""}

    def get_event(self, wid):
        if wid not in self.workflows.keys():
            raise Exception("ID不存在，请先添加任务!")
        if wid in self.event.keys():
            return self.event[wid]
        else:
            event = Event()
            self.event[wid] = event
            return event

    def start(self, wid, pid=0):
        if wid in self.workflows.keys():
            self.workflows[wid].update_pid(pid)
            self.logger.info("Workflow %s 开始运行! " % wid)

    def set_end(self, wid, msg=None):
        if wid in self.workflows.keys():
            self.workflows[wid].end()
            self.workflows.pop(wid)
            if wid in self.event.keys():
                self.return_msg[wid] = {"success": True, "info": msg}
                self.event[wid].set()
                self.event.pop(wid)
            self.logger.info("Workflow %s 运行结束! " % wid)

    def process_end(self, wid):
        if wid in self.workflows.keys():
            error_msg = "Worker %s 进程意外结束.." % wid
            self.workflows[wid].error(error_msg)
            self.workflows.pop(wid)
            if wid in self.event.keys():
                self.return_msg[wid] = {"success": False, "info": error_msg}
                self.event[wid].set()
                self.event.pop(wid)
            self.logger.error("Workflow %s 进程意外结束: %s " % (wid, error_msg))

    def get_msg(self, wid):
        if wid in self.return_msg.keys():
            return self.return_msg.pop(wid)
        else:
            return None

    def keep_alive(self, wid):
        if wid in self.workflows.keys():
            self.workflows[wid].update()
            self.logger.debug("接收到Workflow %s keepavlie信息! " % wid)

    def set_error(self, wid, error_msg):
        if wid in self.workflows.keys():
            self.workflows[wid].error(error_msg)
            self.workflows.pop(wid)
            if wid in self.event.keys():
                self.return_msg[wid] = {"success": False, "info": error_msg}
                self.event[wid].set()
                self.logger.error("event %s set" % wid)
                # self.event.pop(wid)
            self.logger.error("Workflow %s 运行出错: %s " % (wid, error_msg))

    def set_pause(self, wid):
        if wid in self.workflows.keys():
            self.workflows[wid].pause()
            self.logger.info("Workflow %s 暂停运行..." % wid)

    def set_pause_exit(self, wid):
        if wid in self.workflows.keys():
            self.workflows[wid].exit_pause()
            self.logger.info("Workflow %s 退出暂停，继续运行..." % wid)

    def pause_timeout(self, wid):
        if wid in self.workflows.keys():
            self.workflows[wid].pause_timeout()
            self.logger.info("Workflow %s 暂停超时，结束运行..." % wid)

    def set_stop(self, wid):
        if wid in self.workflows.keys():
            self.workflows[wid].stop()
            self.logger.info("Workflow %s 接收中止运行指令..." % wid)


def get_event(wid):
    try:
        event = WorkflowManager().get_event(wid)
    except Exception, e:
        print e
        return None
    else:
        return event


class ManagerProcess(Process):
    def __init__(self, queue):
        super(ManagerProcess, self).__init__()
        self.queue = queue
        self.config = Config()
        self.process = {}
        self.model = CheckModel()

    def run(self):
        super(ManagerProcess, self).run()
        os.setgid(getpwnam(self.config.wpm_user)[3])
        os.setuid(getpwnam(self.config.wpm_user)[2])
        setproctitle.setproctitle("WPM[Process Manager]")
        self.start_thread()
        while True:
            json = self.queue.get()
            try:
                wsheet = Sheet(data=json)
                worker = WorkflowWorker(wsheet)
                worker.start()
            except Exception, e:
                exstr = traceback.format_exc()
                print exstr
                print e
                sys.stdout.flush()
                sys.stderr.flush()
                self.process_end(json["id"])
            else:
                self.process[json["id"]] = worker
                self.process_start(json["id"], worker.pid)

    def process_end(self, wid):
        class WorkerManager(BaseManager):
            pass

        WorkerManager.register("worker")
        wpm_manager = WorkerManager(address=self.config.wpm_listen, authkey=self.config.wpm_authkey)
        wpm_manager.connect()
        worker = wpm_manager.worker()
        worker.process_end(wid)

        class LogManager(BaseManager):
            pass
        LogManager.register("apilog")
        m = LogManager(address=self.config.wpm_logger_listen, authkey=self.config.wpm_logger_authkey)
        m.connect()
        log = m.apilog()
        log.set_end(wid)

    def process_start(self, wid, pid):
        class WorkerManager(BaseManager):
            pass

        WorkerManager.register("worker")
        wpm_manager = WorkerManager(address=self.config.wpm_listen, authkey=self.config.wpm_authkey)
        wpm_manager.connect()
        worker = wpm_manager.worker()
        worker.start(wid, pid)

    def _check_process(self):
        while True:
            time.sleep(1)
            for wid, p in self.process.items():
                if not p.is_alive():
                    p.join()
                    self.process.pop(wid)
                    self.process_end(wid)

    def _check_stop(self):
        results = self.model.find_stop()
        if results:
            for row in results:
                wid = row["workflow_id"]
                if wid in self.process.keys():
                    self.process[wid].action_queue.put("stop")

    def _check_pause(self):
        results = self.model.find_pause()
        if results:
            for row in results:
                wid = row["workflow_id"]
                if wid in self.process.keys():
                    self.process[wid].action_queue.put("pause")

    def _check_exit_pause(self):
        results = self.model.find_exit_pause()
        if results:
            for row in results:
                wid = row["workflow_id"]
                if wid in self.process.keys():
                    self.process[wid].action_queue.put("exit_pause")

    def _check(self):
        while True:
            time.sleep(10)
            self._check_stop()
            self._check_pause()
            self._check_exit_pause()

    def start_thread(self):
        thread = Thread(target=self._check_process, args=(), name='thread-process_check')
        thread.setDaemon(True)
        thread.start()
        thread1 = Thread(target=self._check, args=(), name='thread-stop_pause_check')
        thread1.setDaemon(True)
        thread1.start()


@singleton
class ApiLogManager(object):
    def __init__(self):
        self._workers = {}
        self._running_workers = {}
        self.logger = Logger(log_type="API_LOG_MANAGER")
        self.config = Config()

    def add_log(self, data):
        if data["api"] in self.config.update_exclude_api:
            self.logger.info("Worker %s  API: %s API在排除更新列表中,忽略..." % (data["task_id"], data["api"]))
            return
        if data["task_id"] in self._workers.keys():
            self._workers[data["task_id"]].add_log(data)
            self.logger.info("Worker %s  更新进度.. " % data["task_id"])
        elif data["task_id"] in self._running_workers.keys():
            self._running_workers[data["task_id"]].add_log(data)
            self.logger.info("Worker %s  更新进度.. " % data["task_id"])
        else:
            worker = LogWorker(data["task_id"])
            worker.add_log(data)
            self._workers[data["task_id"]] = worker
            self.logger.info("新Worker %s  API: %s 开始运行... " % (data["task_id"], data["api"]))

    def get_worker(self):
        for wid, worker in self._workers.items():
            self._workers.pop(wid)
            if not worker.is_end:
                self._running_workers[wid] = worker
            return worker
        return None

    def set_end(self, wid):
        if wid in self._workers.keys():
            self._workers[wid].set_end()
        if wid in self._running_workers.keys():
            self._running_workers[wid].set_end()
            self._running_workers.pop(wid)
        self.logger.info("Worker %s 运行完成." % wid)


class ApiLogProcess(Process):
    def __init__(self, **kwargs):
        super(ApiLogProcess, self).__init__(**kwargs)
        self.config = Config()
        self._log_date = None

    def _check_log(self, log_manager):
        while True:
            if self._log_date != time.strftime('%Y%m%d', time.localtime(time.time())):
                self._log_date = time.strftime('%Y%m%d', time.localtime(time.time()))
                log = os.path.join(self.config.UPDATE_LOG, "%s.%s.log" % (self._log_date, hostname))
                if not os.path.exists(self.config.UPDATE_LOG):
                    os.mkdir(self.config.UPDATE_LOG)
                so = file(log, 'a+')
                se = file(log, 'a+', 0)
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())
            worker = log_manager.get_worker()
            if worker:
                worker.run()
            else:
                gevent.sleep(1)

    def start_thread(self, log_manager):
        thread = Thread(target=self._check_log, args=(log_manager, ), name='thread-API_check')
        thread.setDaemon(True)
        thread.start()

    def run(self):
        super(ApiLogProcess, self).run()
        setproctitle.setproctitle("WPM[API LOG manager]")
        log_manager = ApiLogManager()
        self.start_thread(log_manager)

        class ListenerManager(BaseManager):
            pass
        ListenerManager.register('apilog', ApiLogManager)
        m = ListenerManager(address=self.config.wpm_logger_listen, authkey=self.config.wpm_logger_authkey)
        s = m.get_server()
        s.serve_forever()
