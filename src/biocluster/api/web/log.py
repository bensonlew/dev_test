# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import sys
import gevent
import urllib2
import datetime
from biocluster.core.function import hostname
from biocluster.config import Config
import web
import time
from gevent.pool import Group
import json
import os
from biocluster.core.function import get_clsname_form_path
import importlib
import traceback
import urllib

config = Config()
db = config.get_db()


class LogManager(object):
    """
    远程API更新日志管理器,负责管理每个任务的日志
    """
    def __init__(self):
        self._running_task = {}
        self.from_time = None
        self.api = None

    def update_as_service(self):
        timestr = ""
        while True:

            if timestr != time.strftime('%Y%m%d', time.localtime(time.time())):
                timestr = time.strftime('%Y%m%d', time.localtime(time.time()))
                log = self.get_log_path()
                so = file(log, 'a+')
                se = file(log, 'a+', 0)
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())
                self.log("开始监控状态更新")

            ids = self.get_task_ids()
            if len(ids) > 0:
                for tid in ids:
                    if tid not in self._running_task.keys():
                        self._running_task[tid] = gevent.spawn(TaskLog(tid).update)
                    else:
                        if self._running_task[tid].ready():
                            self._running_task[tid] = gevent.spawn(TaskLog(tid).update)
                for key in self._running_task.keys():
                    if key not in ids:
                        if self._running_task[key].ready():
                            del self._running_task[key]
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
        sql = "SELECT DISTINCT task_id from apilog where api <> 'test' and success=0 and reject=0 and" \
              "(has_upload=0  or (has_upload=1 and uploaded=1))"
        if self.api:
            sql += " and api=%s" % self.api
        results = db.query(sql)
        ids = []
        for r in results:
            ids.append(r.task_id)
        return ids

    @staticmethod
    def get_log_path():
        timestr = time.strftime('%Y%m%d', time.localtime(time.time()))
        if not os.path.exists(Config().UPDATE_LOG):
            os.mkdir(Config().UPDATE_LOG)
        log = os.path.join(Config().UPDATE_LOG, "%s.log" % timestr)
        return log

    @staticmethod
    def log(info):
        print("%s\t%s" % (datetime.datetime.now(), info))
        sys.stdout.flush()


class TaskLog(object):
    def __init__(self, task_id):
        self._task_id = task_id
        # self.update_ids = []
        self._data = sorted(self.get_task_data(), key=lambda d: d.id)
        self._end = False

    def update(self):
        for log_data in self._data:
            class_name = get_clsname_form_path(log_data.api, tp="")
            # main = sys.modules["mbio.api.web.%s" % log_data.api]
            try:
                main = importlib.import_module("mbio.api.web.%s" % log_data.api.lower())
                if hasattr(main, class_name):
                    api = getattr(main, class_name)
                    log = api(log_data)
                    log.update()
                    if log.failed:
                        self.log("task_id: %s  log_id: %s   停止更新当前任务日志" % (log_data.task_id, log_data.id))
                        break
                else:
                    self.log("task_id: %s  log_id: %s 没有找到API模块:%s" % (log_data.task_id, log_data.id, class_name))
                    break
            except Exception, e:
                exstr = traceback.format_exc()
                print exstr
                self.log("task_id: %s  log_id: %s  导入API模块出错:%s" % (log_data.task_id, log_data.id, e))
                data = {
                    "failed_times": 1,
                    "reject": 1
                }
                myvar = dict(id=log_data.id)
                try:
                    db.update("apilog", vars=myvar, where="id = $id", **data)
                except Exception, e:
                    self.log("数据库更新错误:%s" % e)
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
            # self.update_ids.append(result.id)
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
        self._url = ""
        self._post_data = ""

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
                self._reject = 1
                break
            try:
                if self._success == 0:
                    gevent.sleep(config.UPDATE_RETRY_INTERVAL)
                self._try_times += 1
                response = self.send()
                code = response.getcode()
                response_text = response.read()
                print("Return page:\n%s" % response_text)
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
                try:
                    response_json = json.loads(response_text)
                except Exception, e:
                    self._response_code = code
                    self._response = response_text
                    self._success = 0
                    self._failed_times += 1
                    self.log("提交失败: 返回数据类型不正确 %s" %  e)
                else:
                    self._response_code = code
                    self._response = response_text
                    if response_json["success"] == "true" \
                            or response_json["success"] is True or response_json["success"] == 1:
                        self._success = 1
                        self.log("提交成功")
                    else:
                        self._success = 0
                        self._failed_times += 1
                        self._reject = 1
                        self.log("提交被拒绝，终止提交:%s" % response_json["message"])
                    break
        self._end = True
        self.save()

    def send(self):
        http_handler = urllib2.HTTPHandler(debuglevel=1)
        https_handler = urllib2.HTTPSHandler(debuglevel=1)
        opener = urllib2.build_opener(http_handler, https_handler)
        urllib2.install_opener(opener)
        request = urllib2.Request(self._url, self._post_data)
        response = urllib2.urlopen(request)
        return response

    def log(self, info):
        print("%s\ttask:%s\tapi:%s\tlog_id:%s\t%s" % (datetime.datetime.now(),
                                                      self.data.task_id, self.__class__.__name__, self._id, info))
        sys.stdout.flush()

    def save(self):
        data = {
            "success": self._success,
            "last_update": datetime.datetime.now(),
            "server": hostname,
            "response": str(self._response),
            "response_code": self._response_code,
            "failed_times": self._failed_times,
            "reject": self._reject
        }
        myvar = dict(id=self._id)
        try:
            db.update("apilog", vars=myvar, where="id = $id", **data)
        except Exception, e:
            self.log("数据库更新错误:%s" % e)

    @property
    def post_data(self):
        data = json.loads(self.data.data)
        return urllib.urlencode(data)
