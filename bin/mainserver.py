# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from multiprocessing.managers import BaseManager
from multiprocessing import Process, Queue, Event
from biocluster.core.function import load_class_by_path, daemonize, hostname
from biocluster.core.singleton import singleton
import MySQLdb as mdb
from biocluster.wsheet import Sheet
from biocluster.config import Config
import time
import os


@singleton
class Manager(object):
    def __init__(self):
        self._workers = {}

    def add(self, json):
        """


        :return:
        """

    def get(self, id):

    def get_data(self, id):



def get_event(id):

    manager = Manager()






class Worker(object):
    """
    管理
    """
    def __init__(self, json):
        self.event =Event()
        self.sheet = Sheet(data=json)
        self.process = None


    def run(self):


class WorkerProcess(Process):
    def __init__(self, ):
        super(WorkerProcess, self).__init__()
        self.wj = wj

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