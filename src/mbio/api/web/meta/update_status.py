# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.api.web.log import Log, config
import gevent
from biocluster.config import Config
import json
from bson.objectid import ObjectId
from types import StringTypes
import datetime
import urlparse


class UpdateStatus(Log):

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._config = Config()
        self._task_id = self.data.task_id
        self.db = self._config.get_db()
        self._mongo_client = self._config.mongo_client
        self.mongodb = self._mongo_client["sanger"]
        self._sheetname = "update_info"

    def update(self):
        table_id = self.get_otu_id()
        while True:
            self._failed = False
            try:
                my_table_id = json.loads(table_id)
            except Exception:
                self.log("update_info:{}格式不正确".format(table_id))
                self._success = 0
                self._failed = True
                self._reject = 1
                break
            if self._try_times >= config.UPDATE_MAX_RETRY:
                self.log("尝试提交%s次任务成功，终止尝试！" % self._try_times)
                self._failed = True
                self._reject = 1
                break
            try:
                if self._success == 0:
                    gevent.sleep(config.UPDATE_RETRY_INTERVAL)
                self._try_times += 1
                if my_table_id:
                    url_data = urlparse.parse_qs(self.data.data)
                    statu = url_data["content"][0]
                    json_data = json.loads(statu, object_hook=date_hook)
                    if "stage" in json_data.keys():
                        status = json_data["stage"]["status"]
                        desc = json_data["stage"]["error"]
                        create_time = json_data["stage"]["created_ts"]
                        self.update_log(my_table_id, status, desc, create_time)
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
                self._failed = True
                self._failed_times += 1
                self.log("提交失败: %s" % e)
            else:
                self._success = 1
                self._failed = False
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
                # 返回mysql的workflow表的json这一列的option字段下的update_api字段下的值
            else:
                self.log("没有找到对应的任务:%s" % self._task_id)
        except Exception, e:
            self.log("任务ID查询异常: %s" % e)
        return False

    def update_log(self, id_value, status, desc, create_time):
        # id_value  {表id:表名, 表id: 表名,...}
        for k in id_value:
            obj_id = k
            dbname = id_value[k]
            collection = self.mongodb[dbname]
            if not isinstance(obj_id, ObjectId):
                if isinstance(obj_id, StringTypes):
                    obj_id = ObjectId(obj_id)
                else:
                    raise Exception("{}的值必须为ObjectId对象或其对应的字符串!".format(self._sheetname))
            create_time = str(create_time)
            if status == "finish":
                status = "end"
                desc = ""
            data = {
                "status": status,
                "desc": desc,
                "created_ts": create_time
            }
            collection.find_one_and_update({"_id": obj_id}, {'$set': data}, upsert=True)

            # 新建或更新sg_status表
            collection = self.mongodb['sg_status']
            if status == "start":
                insert_data = {
                    "table_id": obj_id,
                    "type_name": dbname,
                    "status": "start",
                    "is_new": "new",
                    "desc": desc
                }
                collection.insert_one(insert_data)
            else:
                insert_data = {
                    "status": status,
                    "desc": desc
                }
                collection.find_one_and_update({"table_id": obj_id, "type_name": dbname}, {'$set': insert_data}, upsert=True)
            self._mongo_client.close()


def date_hook(json_dict):
    for (key, value) in json_dict.items():
        try:
            json_dict[key] = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except:
            pass
    return json_dict