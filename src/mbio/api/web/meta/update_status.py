# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.wpm.log import Log
import urllib
from biocluster.config import Config
import json
from bson.objectid import ObjectId
import datetime
import re
import gevent
import urllib2
import sys
from biocluster.core.function import CJsonEncoder


class UpdateStatus(Log):

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._config = Config()
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        self._url = "http://www.tsanger.com/api/add_file"
        self.update_info = self.data["content"]["update_info"] if "update_info" in self.data["content"].keys() else None
        self._post_data = "%s&%s" % (self.get_sig(), self.get_post_data())
        self._mongo_client = self._config.mongo_client
        self.mongodb = self._mongo_client[Config().MONGODB]

    def get_post_data(self):
        workflow_id = self.data["content"]["stage"]["task_id"]
        my_id = re.split('_', workflow_id)
        my_id.pop(-1)
        my_id.pop(-1)
        data = dict()
        content = {
            "task_id": "_".join(my_id),
            "stage": self.data["content"]["stage"]
        }
        if 'files' in self.data['content']:
            content['files'] = self.data["content"]["files"]
        if 'dirs' in self.data['content']:
            content['dirs'] = self.data['content']['dirs']
        data['content'] = json.dumps(content, cls=CJsonEncoder)
        return urllib.urlencode(data)

    def update(self):

        while True:
            if self._try_times >= self.config.UPDATE_MAX_RETRY:
                self.logger.info("尝试提交%s次任务成功，终止尝试！" % self._try_times)
                self._failed = True
                self._reject = 1
                break
            try:
                if self._success == 0:
                    # gevent.sleep(self.config.UPDATE_RETRY_INTERVAL)
                    gevent.sleep(3)
                self._try_times += 1
                response = self.send()
                code = response.getcode()
                response_text = response.read()
                self.update_status()
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
                    self.logger.error("提交失败: 返回数据类型不正确 %s ，重试..." %  e)
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

    def update_status(self):
        status = self.data["content"]["stage"]["status"]
        desc = self.data["content"]["stage"]["error"]
        create_time = str(self.data["content"]["stage"]["created_ts"])
        if not self.update_info:
            return
        for obj_id, collection_name in json.loads(self.update_info).items():
            obj_id = ObjectId(obj_id)
            collection = self.mongodb[collection_name]
            if status == "finish":
                data = {
                    "status": "end",
                    "desc": "",
                    "created_ts": create_time
                }
                collection.find_one_and_update({"_id": obj_id}, {'$set': data}, upsert=True)
            collection = self.mongodb['sg_status']
            if status == "start":
                tmp_col = self.mongodb[collection_name]
                try:
                    temp_find = tmp_col.find_one({"_id": obj_id})
                    tb_name = temp_find["name"]
                    temp_params = temp_find['params']
                    submit_location = json.loads(temp_params)['submit_location']
                except:
                    tb_name = ""
                    temp_params = ''
                    submit_location = ''
                tmp_task_id = list()
                print 'update_status task_id:', self.task_id
                tmp_task_id = re.split("_", self.task_id)
                tmp_task_id.pop()
                tmp_task_id.pop()
                insert_data = {
                    "table_id": obj_id,
                    "table_name": tb_name,
                    "task_id": "_".join(tmp_task_id),
                    "type_name": collection_name,
                    "params": temp_params,
                    "submit_location": submit_location,
                    "status": "start",
                    "is_new": "new",
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                collection.insert_one(insert_data)
            elif status == "finish":  # 只能有一次finish状态
                insert_data = {
                    "status": 'end',
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                collection.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                               {'$set': insert_data}, upsert=True)
            else:
                insert_data = {
                    "status": status,
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                collection.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                               {'$set': insert_data}, upsert=True)
            self._mongo_client.close()
