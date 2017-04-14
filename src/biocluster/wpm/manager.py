# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from ..core.singleton import singleton
from multiprocessing import Event, Queue, Process
from .workflow import WorkflowWorker
from ..wsheet import Sheet
from .logger import Logger
from .log import LogWorker
from ..config import Config
from .db import WorkflowModel, CheckModel, ClientKeyModel
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
import hashlib
import threading


@singleton
class WorkflowManager(object):
    def __init__(self):
        self.queue = Queue()
        self.workflows = {}
        self.event = {}
        self.multi_event = {}
        self.return_msg = {}
        self.logger = Logger()
        self.server = None
        self._key = {}
        self._port_list = {}
        self.config = Config()
        self.port_lock = threading.Lock()

    def add_task(self, json):
        try:
            if not isinstance(json, dict) or "id" not in json.keys():
                self.logger.error("add workflow %s format error!" % json)
                return {"success": False, "info": "json格式错误"}
            json["WPM"] = True
            json["endpoint"] = "tcp://{}:{}".format(self.__get_ip(), self.__get_port(json["id"]))
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
            return {"success": True, "info": "任务提交成功."}
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 add_task: %s" % e)

    def __get_port(self, wid):
        if wid in self._port_list.keys():
            return self._port_list[wid]
        port = self.config.get_listen_port()
        if port in self._port_list.values():
            return self.__get_port(wid)
        else:
            with self.port_lock:
                self._port_list[wid] = port
            return port

    def __get_ip(self):
        return self.config.LISTEN_IP

    def get_event(self, *wid):
        for eid in wid:
            if eid not in self.workflows.keys():
                raise Exception("ID%s不存在，请先添加任务!" % eid)
        if len(wid) == 1:
            my_id = wid[0]
        else:
            ids = list(wid)
            ids.sort()
            my_id = hashlib.sha1("".join([str(i) for i in ids])).hexdigest()

        if my_id in self.event.keys():
            return self.event[my_id]
        else:
            event = Event()
            self.event[my_id] = event
            if len(wid) > 1:
                for i in wid:
                    self.multi_event[my_id] = {}
                    self.multi_event[my_id][i] = False
            return event

    def start(self, wid, pid=0):
        try:
            if wid in self.workflows.keys():
                self.workflows[wid].update_pid(pid)
                self.logger.info("Workflow %s 开始运行! " % wid)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 start: %s" % e)

    def set_end(self, wid, msg=None):
        try:
            if wid in self.workflows.keys():
                model = self.workflows.pop(wid)
                if model:
                    model.end()
                if wid in self.event.keys():
                    self.return_msg[wid] = {"success": True, "info": msg}
                    event = self.event.pop(wid)
                    if event:
                        event.set()
                if wid in self._port_list.keys():
                    self._port_list.pop(wid)
                for m_id in self.multi_event.keys():
                    all_over = True
                    for mw_id in self.multi_event[m_id]:
                        if mw_id == wid:
                            self.multi_event[m_id][mw_id] = {"success": True, "info": "运行结束!"}
                        if self.multi_event[m_id][mw_id] is False:
                            all_over = False
                    if all_over:
                        if m_id in self.event:
                            event = self.event.pop(m_id)
                            if event:
                                event.set()
                self.logger.info("Workflow %s 运行结束! " % wid)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 set_end: %s" % e)

    def process_end(self, wid):
        try:
            if wid in self.workflows.keys():
                error_msg = "Worker %s 进程意外结束.." % wid
                model = self.workflows.pop(wid)
                if model:
                    model.error(error_msg)

                if wid in self._port_list.keys():
                    self._port_list.pop(wid)

                if wid in self.event.keys():
                    self.return_msg[wid] = {"success": False, "info": error_msg}
                    event = self.event.pop(wid)
                    if event:
                        event.set()
                for m_id in self.multi_event.keys():
                    all_over = True
                    for mw_id in self.multi_event[m_id]:
                        if mw_id == wid:
                            self.multi_event[m_id][mw_id] = {"success": False, "info": "进程意外结束"}
                        if self.multi_event[m_id][mw_id] is False:
                            all_over = False
                    if all_over:
                        if m_id in self.event:
                            event = self.event.pop(m_id)
                            if event:
                                event.set()
                self.logger.error("Workflow %s 进程意外结束: %s " % (wid, error_msg))
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 process_end: %s" % e)

    def get_msg(self, *wid):
        try:
            if len(wid) == 1:
                if wid[0] in self.return_msg.keys():
                    return self.return_msg.pop(wid[0])
                else:
                    return None
            else:
                ids = list(wid)
                ids.sort()
                my_id = hashlib.sha1("".join([str(i) for i in ids])).hexdigest()

                if my_id in self.multi_event.keys():
                    msg = {}
                    for mw_id in self.multi_event[my_id]:
                        if self.multi_event[my_id][mw_id] is not False:
                            msg[mw_id] = self.multi_event[my_id][mw_id]
                    self.multi_event.pop(my_id)
                    return msg
                else:
                    return None
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 get_msg: %s" % e)

    def keep_alive(self, wid):
        try:
            if wid in self.workflows.keys():
                self.workflows[wid].update()
                self.logger.debug("接收到Workflow %s keepavlie信息! " % wid)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 keep_alive: %s" % e)

    def set_error(self, wid, error_msg):
        try:
            if wid in self.workflows.keys():
                model = self.workflows.pop(wid)
                if model:
                    model.error(error_msg)

                if wid in self._port_list.keys():
                    self._port_list.pop(wid)

                if wid in self.event.keys():
                    self.return_msg[wid] = {"success": False, "info": error_msg}
                    event = self.event.pop(wid)
                    if event:
                        event.set()
                for m_id in self.multi_event.keys():
                    all_over = True
                    for mw_id in self.multi_event[m_id]:
                        if mw_id == wid:
                            self.multi_event[m_id][mw_id] = {"success": False, "info": error_msg}
                        if self.multi_event[m_id][mw_id] is False:
                            all_over = False
                    if all_over:
                        if m_id in self.event:
                            event = self.event.pop(m_id)
                            if event:
                                event.set()
                self.logger.error("Workflow %s 运行出错: %s " % (wid, error_msg))
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 set_error: %s" % e)

    def set_pause(self, wid):
        try:
            if wid in self.workflows.keys():
                self.workflows[wid].pause()
                self.logger.info("Workflow %s 暂停运行..." % wid)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 set_pause: %s" % e)

    def set_pause_exit(self, wid):
        try:
            if wid in self.workflows.keys():
                self.workflows[wid].exit_pause()
                self.logger.info("Workflow %s 退出暂停，继续运行..." % wid)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 set_pause_exit: %s" % e)

    def pause_timeout(self, wid):
        try:
            if wid in self.workflows.keys():
                self.workflows[wid].pause_timeout()
                self.logger.info("Workflow %s 暂停超时，结束运行..." % wid)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 pause_timeout: %s" % e)

    def set_stop(self, wid):
        try:
            if wid in self.workflows.keys():
                self.workflows[wid].stop()
                self.logger.info("Workflow %s 接收中止运行指令..." % wid)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 set_stop: %s" % e)

    def get_key(self, client):
        if client not in self._key.keys():
            model = ClientKeyModel()
            self._key[client] = model.find_key(client)
        return self._key[client]


def get_event(*wid):
    try:
        if len(wid) == 1:
            event = WorkflowManager().get_event(wid[0])
        else:
            event = WorkflowManager().get_event(*wid)

    except Exception, e:
        exstr = traceback.format_exc()
        print e
        print exstr
        sys.stdout.flush()
        return None
    else:
        return event


class ManagerProcess(Process):
    def __init__(self, queue):
        super(ManagerProcess, self).__init__()
        self.queue = queue
        self.config = Config()
        self.process = {}
        self._model = None

    @property
    def model(self):
        if not self._model:
            self._model = CheckModel()
        return self._model

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
        del worker

        class LogManager(BaseManager):
            pass
        LogManager.register("apilog")
        m = LogManager(address=self.config.wpm_logger_listen, authkey=self.config.wpm_logger_authkey)
        m.connect()
        log = m.apilog()
        log.set_end(wid)
        del log

    def process_start(self, wid, pid):
        class WorkerManager(BaseManager):
            pass

        WorkerManager.register("worker")
        wpm_manager = WorkerManager(address=self.config.wpm_listen, authkey=self.config.wpm_authkey)
        wpm_manager.connect()
        worker = wpm_manager.worker()
        worker.start(wid, pid)
        del worker

    def _check_process(self):
        while True:
            time.sleep(1)
            for wid, p in self.process.items():
                if not p.is_alive():
                    p.join()
                    try:
                        self.process_end(wid)
                    except Exception, e:
                        exstr = traceback.format_exc()
                        print exstr
                        print e
                        sys.stdout.flush()
                    self.process.pop(wid)

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
            time.sleep(15)
            try:
                self._check_stop()
                self._check_pause()
                self._check_exit_pause()
            except Exception, e:
                print e
                sys.stdout.flush()
                continue

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
        try:
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
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 add_log: %s" % e)

    def get_worker(self):
        try:
            for wid, worker in self._workers.items():
                self._workers.pop(wid)
                if not worker.is_end:
                    self._running_workers[wid] = worker
                return worker
            return None
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 get_worker: %s" % e)

    def set_end(self, wid):
        try:
            if wid in self._workers.keys():
                self._workers[wid].set_end()
            if wid in self._running_workers.keys():
                self._running_workers[wid].set_end()
                self._running_workers.pop(wid)
            self.logger.info("Worker %s 运行完成." % wid)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("服务异常 set_end: %s" % e)


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
        setproctitle.setproctitle("WPM[API Log Manager]")
        log_manager = ApiLogManager()
        self.start_thread(log_manager)

        class ListenerManager(BaseManager):
            pass
        ListenerManager.register('apilog', ApiLogManager)
        m = ListenerManager(address=self.config.wpm_logger_listen, authkey=self.config.wpm_logger_authkey)
        s = m.get_server()
        s.serve_forever()
