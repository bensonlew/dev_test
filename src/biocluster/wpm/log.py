# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import gevent
from threading import Thread, Lock


class LogWorker(object):
    def __init__(self, wid):
        self.workflow_id = wid
        self.log_list = []
        self.lock = Lock()

    def _loop(self):
        while True:




    def run(self):
