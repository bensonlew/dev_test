# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from ..core.singleton import singleton
from multiprocessing import Event
from multiprocessing.managers import BaseManager
from .workflow import WorkflowWorker
from ..wsheet import Sheet
from threading import Lock, Thread
import logging
from ..config import Config
import time
import os


class Logger(object):
    """
    日志类
    """

    def __init__(self, lock, log_type="WPM"):
        """
        """
        self.config = Config()
        log_level = {'debug': logging.DEBUG,
                     'info': logging.INFO,
                     'warning': logging.WARNING,
                     'error': logging.ERROR,
                     'critical': logging.CRITICAL}
        self.format = self.config.LOG_FORMAT
        self.formatter = logging.Formatter(self.format, "%Y-%m-%d %H:%M:%S")
        self.level = log_level[self.config.LOG_LEVEL.lower()]
        self.streem_on = self.config.LOG_STREEM
        self._logger_date = time.strftime('%Y%m%d', time.localtime(time.time()))
        self.file_handler = None
        self.log_type = log_type
        self.lock = lock
        if self.log_type == "WPM":
            self.path_dir = self.config.wpm_log_file
        else:
            self.path_dir = self.config.UPDATE_LOG
        if self.streem_on:
            self.stream_handler = logging.StreamHandler()
            self.stream_handler.setLevel(self.level)
            self.stream_handler.setFormatter(self.formatter)
        self.logger = self.get_logger(self.log_type)

    def get_logger(self, name=""):
        """
        返回一个logger对象

        :param name: logger名字
        """
        logger = logging.getLogger(name)
        logger.propagate = 0
        if self.streem_on:
            logger.addHandler(self.stream_handler)
        self._add_handler(logger)
        return logger

    def _add_handler(self, logger):
        """
        """
        logger.setLevel(self.level)
        now_date = time.strftime('%Y%m%d', time.localtime(time.time()))
        if now_date != self._logger_date or self.file_handler is None:
            self._logger_date = now_date
            logger.removeHandler(self.file_handler)
            file_path = os.path.join(self.path_dir, now_date + ".log")
            self.file_handler = logging.FileHandler(file_path)
            self.file_handler.setLevel(self.level)
            self.file_handler.setFormatter(self.formatter)
            logger.addHandler(self.file_handler)

    def debug(self, *args,  **kargs):
         self.lock.acquire()
         self._add_handler(self.logger)
         self.logger.debug(*args,  **kargs)
         self.lock.release()

    def info(self, *args, **kargs):
        self.lock.acquire()
        self._add_handler(self.logger)
        self.logger.info(*args, **kargs)
        self.lock.release()

    def warning(self, *args, **kargs):
        self.lock.acquire()
        self._add_handler(self.logger)
        self.logger.warning(*args, **kargs)
        self.lock.release()

    def error(self, *args, **kargs):
        self.lock.acquire()
        self._add_handler(self.logger)
        self.logger.error(*args, **kargs)
        self.lock.release()

    def critical(self, *args, **kargs):
        self.lock.acquire()
        self._add_handler(self.logger)
        self.logger.critical(*args, **kargs)
        self.lock.release()


@singleton
class WorkflowManager(object):
    def __init__(self):
        self.workflows = {}
        self.event = {}
        self.return_msg = {}
        self.lock = Lock()
        self.logger = Logger(self.lock)

    def add_task(self, json):
        if not isinstance(json, dict) or "id" not in json.keys():
            self.logger.error("add workflow %s format error!" % json)
            return False, "json格式错误"
        json["WPM"] = True
        wsheet = Sheet(data=json)
        if wsheet.id in self.workflows.keys():
            self.logger.error("Workflow %s has already run! " % wsheet.id)
            return False, "Workflow %s正在运行，不能重复添加!" % wsheet.id

        process = WorkflowWorker(wsheet, name="Workflow[%s] worker" % wsheet.id)
        if process.model.find():
            self.logger.error("workflow id is already exists!")
            return False, "Workflow %s已经存在，不能重复运行!" % wsheet.id
        self.workflows[wsheet.id] = process
        self.lock.acquire()
        process.start()
        process.model.save()
        self.lock.release()
        self.logger.info("Workflow[%s] worker start run" % wsheet.id)
        return True

    def get_event(self, wid):
        if wid not in self.workflows.keys():
            raise Exception("ID不存在，请先添加任务!")
        if wid in self.event.keys():
            return self.event[wid]
        else:
            event = Event()
            self.event[id] = event
            return event

    def set_end(self, wid, msg=None):
        if wid in self.workflows.keys():
            self.workflows[wid].model.end()
            self.return_msg[wid] = msg
            if wid in self.event.keys():
                self.event[wid].set()
                self.event.pop(wid)

    def get_msg(self, wid):
        if wid in self.return_msg.keys():
            return self.return_msg.pop(wid)
        else:
            return None

    def keep_alive(self, wid):
        if wid in self.workflows.keys():
            self.workflows[wid].model.update()

    def set_error(self, wid, error_msg):
        if wid in self.workflows.keys():
            self.workflows[wid].model.update(error_msg)

    def set_pause(self, wid):
        if wid in self.workflows.keys():
            self.workflows[wid].model.pause()

    def set_pause_exit(self, wid):
        if wid in self.workflows.keys():
            self.workflows[wid].model.exit_pause()

    def pause_timeout(self, wid):
        if wid in self.workflows.keys():
            self.workflows[wid].model.pause_timeout()


def get_event(wid):
    try:
        event = WorkflowManager().get_event(wid)
    except Exception, e:
        return False, "Error %d: %s" % (e.args[0], e.args[1])
    else:
        return event


class ListenManager(BaseManager):
    pass