# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
# last_modified = shenghe
# last_modified = moli.zhou
import urllib
import json
import datetime
import re
import gevent
import urllib2
import sys
from bson.objectid import ObjectId
from biocluster.wpm.log import Log
from biocluster.config import Config
from biocluster.core.function import CJsonEncoder, filter_error_info


class UpdateStatus(Log):
    """
    meta的web api，用于更新sg_status表并向前端发送状态信息和文件上传信息
    一般可web api功能可从此处继承使用，需要重写__init__方法
    """

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._config = Config()
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://www.sanger.com/api/add_file"
        self._post_data = "%s&%s" % (self.get_sig(), self.get_post_data())
        self._mongo_client = self._config.mongo_client
        self.mongodb = self._mongo_client[Config().MONGODB+'_paternity_test_v2']

    def update(self):
        self.update_status()

    def update_status(self):
        status = self.data["content"]["stage"]["status"]
        desc = filter_error_info(self.data["content"]["stage"]["error"])
        create_time = str(self.data["content"]["stage"]["created_ts"])
        if not self.update_info:
            return
        for obj_id, collection_name in json.loads(self.update_info).items():
            obj_id = ObjectId(obj_id)
            collection = self.mongodb[collection_name]
            if status != "start":
                data = {
                    "status": "end" if status == 'finish' else status,
                    "desc": desc,
                    "created_ts": create_time
                }
                collection.find_one_and_update({"_id": obj_id}, {'$set': data}, upsert=True)
            sg_status_col = self.mongodb['sg_pt_datasplit']
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
                sg_status_col.insert_one(insert_data)
            elif status == "finish":  # 只能有一次finish状态
                insert_data = {
                    "status": 'end',
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                sg_status_col.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                                  {'$set': insert_data}, upsert=True)
            else:
                insert_data = {
                    "status": status,
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                sg_status_col.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                                  {'$set': insert_data}, upsert=True)
            self._mongo_client.close()
