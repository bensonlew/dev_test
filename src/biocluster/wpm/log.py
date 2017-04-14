# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import gevent
from threading import Lock
import importlib
from ..core.function import get_clsname_form_path, CJsonEncoder
import traceback
from ..config import Config
import urllib2
import json
import urllib
from .logger import Logger
from .db import ApiLogModel
import random
import time
import hashlib
import sys


class LogWorker(object):
    def __init__(self, wid):
        self._id = wid
        self._log_list = []
        self.lock = Lock()
        self._end = False
        self._start = False
        self.logger = Logger(log_type="Log_Worker[%s]" % wid)
        self._log_num = 0

    @property
    def id(self):
        return self._id

    def set_end(self):
        self._end = True

    def add_log(self, log_data):
        self.lock.acquire()
        self._log_num += 1
        log_data["log_count_num"] = self._log_num
        self._log_list.append(log_data)
        self.lock.release()
        return self._id

    @property
    def is_end(self):
        return self._end

    @property
    def is_start(self):
        return self._start

    def _loop(self):
        self.logger.info("Workflow %s 开始更新日志..", self.id)
        while True:
            if self._end and len(self._log_list) == 0:
                break
            if len(self._log_list) > 0:
                self.lock.acquire()
                data = self._log_list.pop(0)
                self.lock.release()
                self._run_log(data)
            gevent.sleep(1)
        self.logger.info("Workflow %s 日志更新完成", self.id)
        self._end = True

    def _run_log(self, log_data):
        class_name = get_clsname_form_path(log_data["api"], tp="")
        try:
            main = importlib.import_module("mbio.api.web.%s" % log_data["api"].lower())
            if hasattr(main, class_name):
                api = getattr(main, class_name)
                log = api(log_data)
                log.update()
            else:
                self.logger.error("没有找到API模块:%s" % class_name)
        except Exception, e:
            exstr = traceback.format_exc()
            print(exstr)
            sys.stdout.flush()
            self.logger.error("task_id: %s  api: %s  导入API模块出错:%s" % (log_data['task_id'], log_data["api"], e))

    def run(self):
        self._start = True
        gevent.spawn(self._loop)


class Log(object):

    def __init__(self, data):
        self.log_count_num = data["log_count_num"]
        self._data = data["data"]
        self.task_id = data["task_id"]
        self.api = data["api"]
        self.update_info = data["update_info"] if "update_info" in data.keys() else None
        self._last_update = None
        self._response = ""
        self._response_code = 0
        self._failed_times = 0
        self._try_times = 0
        self._success = -1
        self._reject = 0
        self._end = False
        self._failed = False
        self._url = ""
        self._post_data = ""
        self.config = Config()
        self.logger = Logger(log_type="Log[%s:%s]" % (self.task_id, self.log_count_num))
        self.model = ApiLogModel(self)
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://www.sanger.com/api/add_task_log"
        self._post_data = "%s&%s" % (self.get_sig(), self.post_data)

    @property
    def failed(self):
        return self._failed

    @property
    def end(self):
        return self._end

    @property
    def data(self):
        return self._data

    @property
    def response(self):
        return self._response

    @property
    def response_code(self):
        return self._response_code

    @property
    def post_data(self):
        my_content = self.data["content"]
        my_data = dict()
        my_data["content"] = json.dumps(my_content, cls=CJsonEncoder)
        return urllib.urlencode(my_data)

    def update(self):

        while True:
            if self._try_times >= self.config.UPDATE_MAX_RETRY:
                self.logger.info("尝试提交%s次任务成功，终止尝试！" % self._try_times)
                self._failed = True
                self._reject = 1
                break
            try:
                if self._success == 0:
                    gevent.sleep(self.config.UPDATE_RETRY_INTERVAL)
                self._try_times += 1
                response = self.send()
                code = response.getcode()
                response_text = response.read()
                print "Return page:\n%s" % response_text
                sys.stdout.flush()
            except urllib2.HTTPError, e:
                self._success = 0
                self._failed_times += 1
                self._response_code = e.code
                self.logger.warning("提交失败：%s, 重试..." % e)
            except Exception, e:
                self._success = 0
                self._failed_times += 1
                self.logger.warning("提交失败: %s, 重试..." % e)
            else:
                try:
                    response_json = json.loads(response_text)
                except Exception, e:
                    self._response_code = code
                    self._response = response_text
                    self._success = 0
                    self._failed_times += 1
                    self.logger.error("提交失败: 返回数据类型不正确 %s ，重试..." % e)
                else:
                    self._response_code = code
                    self._response = response_text
                    if response_json["success"] == "true" \
                            or response_json["success"] is True or response_json["success"] == 1:
                        self._success = 1
                        self.logger.info("提交成功")
                    else:
                        self._success = 0
                        self._failed_times += 1
                        self._reject = 1
                        self._failed = True
                        self.logger.error("提交被拒绝，终止提交:%s" % response_json["message"])
                    break
        self._end = True
        self.model.save()
        # self.save()

    def send(self):
        http_handler = urllib2.HTTPHandler(debuglevel=1)
        https_handler = urllib2.HTTPSHandler(debuglevel=1)
        opener = urllib2.build_opener(http_handler, https_handler)
        urllib2.install_opener(opener)
        request = urllib2.Request(self._url, self._post_data)
        response = urllib2.urlopen(request)
        sys.stdout.flush()
        sys.stderr.flush()
        return response

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
