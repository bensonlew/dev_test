# -*- coding: utf-8 -*-
# __author__ = 'guoquan'


from .manager import ApiLogProcess, get_event, ManagerProcess, WorkflowManager
from ..config import Config
import setproctitle
import time
import os
import sys
from threading import Thread
from multiprocessing.managers import BaseManager
import traceback
from ..core.singleton import singleton
import datetime
from ..core.function import hostname


def write_log(info):
    print("%s\t%s" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), info))
    sys.stdout.flush()


def start():
    server = MainServer()
    server.start()


@singleton
class MainServer(object):
    def __init__(self):
        self.api_log_server = ApiLogProcess()
        wm = WorkflowManager()
        self.manager_server = ManagerProcess(wm.queue)
        self.config = Config()
        self._log_date = None

    def _check_date(self):
        while True:
            if self._log_date != time.strftime('%Y%m%d', time.localtime(time.time())):
                self._log_date = time.strftime('%Y%m%d', time.localtime(time.time()))
                log = os.path.join(self.config.wpm_log_file, "%s.%s.log" % (self._log_date, hostname))
                so = file(log, 'a+')
                se = file(log, 'a+', 0)
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())
            time.sleep(60)

    def start_thread(self):
        thread = Thread(target=self._check_date, args=(), name='thread-date_check')
        thread.setDaemon(True)
        thread.start()

    def start(self):
        # start check thread
        setproctitle.setproctitle("WPM[main server]")
        self.start_thread()
        time.sleep(2)
        # start process manager
        write_log("启动进程管理器...")
        self.manager_server.start()

        # start api server
        write_log("启动API LOG监听...")
        self.api_log_server.start()

        # start main server
        write_log("启动WPM主服务监听...")

        class ListenerManager(BaseManager):
            pass
        ListenerManager.register('worker', WorkflowManager)
        ListenerManager.register('get_event', get_event)
        try:
            m = ListenerManager(address=self.config.wpm_listen, authkey=self.config.wpm_authkey)
            s = m.get_server()
            s.serve_forever()
        except Exception:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            sys.stderr.flush()
            self.manager_server.terminate()
            self.api_log_server.terminate()
            sys.exit(1)
