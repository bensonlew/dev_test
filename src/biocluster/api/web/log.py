# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import sys
import gevent
import urllib2
import datetime
from biocluster.core.function import hostname, daemonize
from biocluster.config import Config
import web
import random
import time
import hashlib
import urllib
from gevent.pool import Group
import json
import os

config = Config()
db = config.get_db()


class LogManager(object):
    def __init__(self):
        self._running_task = {}
        self.from_time = None
        self.api = None

    def update_as_service(self):
        timestr = ""
        while True:

            if timestr != time.strftime('%Y%m', time.localtime(time.time())):
                timestr = time.strftime('%Y%m', time.localtime(time.time()))
                log = self.get_log_path()
                so = file(log, 'a+')
                se = file(log, 'a+', 0)
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())

            ids = self.get_task_ids()
            if len(ids) > 0:
                for tid in ids:
                    if tid not in self._running_task.keys():
                        self._running_task[tid] = gevent.spawn(TaskLog(tid).update)
            gevent.sleep(config.UPDATE_FREQUENCY)

    def update(self):

        ids = self.get_task_ids()
        if len(ids) > 0:
            group = Group()
            for tid in ids:
                if tid not in self._running_task.keys():
                    self._running_task[tid] = gevent.spawn(TaskLog(tid).update)
                    group.add(self._running_task[tid])
            group.join()

    def get_task_ids(self):
        sql = "SELECT DISTINCT task_id from apilog where success=0 and reject=0"
        if self.api:
            sql += " and api=%s" % self.api
        results = db.query(sql)
        ids = []
        for r in results:
            ids.append(r.task_id)
        return ids

    @staticmethod
    def get_log_path():
        timestr = time.strftime('%Y%m', time.localtime(time.time()))
        if not os.path.exists(Config().UPDATE_LOG):
            os.mkdir(Config().UPDATE_LOG)
        log = os.path.join(Config().UPDATE_LOG, "%s.log" % timestr)
        return log


class TaskLog(object):
    def __init__(self, task_id):
        self._task_id = task_id
        self._data = sorted(self.get_task_data(), key=lambda d: d.id)
        self._end = False

    def update(self):
        for log_data in self._data:
            main = sys.modules["biocluster.api.web.log"]
            if hasattr(main, log_data.api):
                api = getattr(main, log_data.api)
                log = api(log_data)
                log.update()
                if log.failed:
                    self.log("停止更新当前任务日志")
                    break
            else:
                self.log("没有找到API模块:%s" % log_data.api)
                break
        self._end = True

    def log(self, info):
        print("%s\ttask:%s\t%s\t" % (datetime.datetime.now(), self._task_id, info))
        sys.stdout.flush()

    def get_task_data(self):
        where_dict = dict(task_id=self._task_id, success=0, reject=0)
        results = db.select("apilog", where=web.db.sqlwhere(where_dict))
        data = []
        for result in results:
            data.append(result)
        return data


class Log(object):

    def __init__(self, data):
        self._id = data.id
        self._data = data
        self._last_update = None
        self._response = ""
        self._response_code = 0
        self._failed_times = data.failed_times
        self._try_times = 0
        self._success = -1
        self._reject = 0
        self._end = False
        self._failed = False

    @property
    def failed(self):
        return self._failed

    @property
    def end(self):
        return self._end

    @property
    def data(self):
        return self._data

    def update(self):

        while True:
            if self._try_times >= config.UPDATE_MAX_RETRY:
                self.log("尝试提交%s次任务成功，终止尝试！" % self._try_times)
                self._failed = True
                break
            try:
                if self._success == 0:
                    gevent.sleep(config.UPDATE_RETRY_INTERVAL)
                self._try_times = 1
                code, response = self.send()
            except urllib2.HTTPError, e:
                self._success = 0
                self._failed_times += 1
                self._response_code = e.code
                self.log("提交失败：%s" % e)
            except Exception, e:
                self._success = 0
                self._failed_times += 1
                self.log("提交失败: %s" % e)
            else:
                self._response_code = code
                self._response = response
                if response["success"] == "true" or response["success"] is True or response["success"] == 1:
                    self._success = 1
                    self.log("提交成功")
                else:
                    self._success = 0
                    self._failed_times += 1
                    self._reject = 1
                    self.log("提交被拒绝，终止提交:%s" % response["message"])
                break
        self._end = True
        self.save()

    def send(self):

        return 0, {}

    def log(self, info):
        print("%s\ttask:%s\tapi:%s\tlog_id:%s\t%s" % (datetime.datetime.now(),
                                                      self.data.task_id, self.__class__.__name__, self._id, info))
        sys.stdout.flush()

    def save(self):
        data = {
            "success": self._success,
            "last_update": datetime.datetime.now(),
            "server": hostname,
            "response": json.dumps(self._response),
            "response_code": self._response_code,
            "failed_times": self._failed_times,
            "reject": self._reject
        }
        myvar = dict(id=self._id)
        try:
            db.update("apilog", vars=myvar, where="id = $id", **data)
        except Exception, e:
            self.log("数据库更新错误:%s" % e)


class Sanger(Log):

    def __init__(self, data):
        super(Sanger, self).__init__(data)
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://172.16.3.233/api/add_task_log"

    def send(self):
        # url = "%s?%s" % (self._url, self.get_sig())
        httpHandler = urllib2.HTTPHandler(debuglevel=1)
        httpsHandler = urllib2.HTTPSHandler(debuglevel=1)
        opener = urllib2.build_opener(httpHandler, httpsHandler)
        urllib2.install_opener(opener)
        request = urllib2.Request(self._url, "%s&%s" % (self.get_sig(), self.data.data))
        response = urllib2.urlopen(request)
        json_data = response.read()
        print("Return page:\n%s" % json_data)
        return response.getcode(), json.loads(json_data)

    def get_sig(self):
        nonce = str(random.randint(1000, 10000))
        timestamp = str(int(time.time()))
        x_list = [self._key, timestamp, nonce]
        x_list.sort()
        sha1 = hashlib.sha1()
        map(sha1.update, x_list)
        sig = sha1.hexdigest()
        signature = {
            "client": self._client,
            "nonce": nonce,
            "timestamp": timestamp,
            "signature": sig
        }
        return urllib.urlencode(signature)
