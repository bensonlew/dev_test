# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.api.web.log import Log, config
import gevent
from biocluster.config import Config
import json
from bson.objectid import ObjectId
from types import StringTypes
import datetime
from pymongo import MongoClient
import urlparse


class UpdateStatus(Log):

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._config = Config()
        self._task_id = self.data.task_id
        self.db = self._config.get_db()
        self._mongo_client = MongoClient(self._config.MONGO_URI)
        self.mongodb = self._mongo_client["sanger"]
        self._sheetname = "update_info"

    def update(self):
        otu_id = self.get_otu_id()
        otu_id = json.loads(otu_id)
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
                if otu_id:
                    url_data = urlparse.parse_qs(self.data.data)
                    statu = url_data["content"][0]
                    json_data = json.loads(statu, object_hook=date_hook)
                    if "stage" in json_data.keys():
                        status = json_data["stage"]["status"]
                        desc = json_data["stage"]["error"]
                        create_time = json_data["stage"]["created_ts"]
                        self.update_log(otu_id, status, desc, create_time)
                    else:
                        self._success = 0
                        self._failed = True
                        self._failed_times += 1
                        self._reject = 1
                        break
                else:
                    self._success = 0
                    self._failed = True
                    self._failed_times += 1
                    self._reject = 1
                    break
            except Exception, e:
                self._success = 0
                self._failed_times += 1
                self.log("提交失败: %s" % e)
            else:
                self._success = 1
                self.log("提交成功")
                break
        self._end = True
        self.save()

    def get_otu_id(self):
        try:
            results = self.db.query("SELECT * FROM workflow WHERE workflow_id=$id", vars={'id': self._task_id})
            if len(results) > 0:
                data = results[0]
                json_str = data.json
                json_obj = json.loads(json_str)
                return json_obj["options"][self._sheetname]
            else:
                self.log("没有找到对应的任务:%s" % self._task_id)
        except Exception, e:
            self.log("任务ID查询异常: %s" % e)
        return False

    def update_log(self, id_value, status, desc, create_time):
        for k in id_value:
            obj_id = self.mongodb[k]
            collection = id_value[k]
            if not isinstance(obj_id, ObjectId):
                if isinstance(obj_id, StringTypes):
                    obj_id = ObjectId(obj_id)
                else:
                    raise Exception("{}的值必须为ObjectId对象或其对应的字符串!".format(self._sheetname))
            data = {
                '_id': obj_id,
                "status": status,
                "desc": desc,
                "created_ts": create_time
            }
            collection.find_one_and_update({"_id": obj_id}, {'$set': data}, upsert=True)


def date_hook(json_dict):
    for (key, value) in json_dict.items():
        try:
            json_dict[key] = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except:
            pass
    return json_dict
