# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.wpm.log import Log
import urllib
from biocluster.config import Config
import json
from bson.objectid import ObjectId
import datetime
import re


class UpdateStatus(Log):

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._config = Config()
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://www.sanger.com/api/add_file"
        self.set_post_data()
        self._post_data = "%s&%s" % (self.get_sig(), self.get_post_data())
        self._mongo_client = self._config.mongo_client
        self.mongodb = self._mongo_client[Config().MONGODB]

    def get_post_data(self):
        workflow_id = self.data["content"]["stage"]["task_id"]
        my_id = re.split('_', workflow_id)
        my_id.pop(-1)
        my_id.pop(-1)
        data = dict()
        data['content'] = json.dumps({
            "task_id": "_".join(my_id),
            "files":self.data["content"]["files"],
            "dirs": self.data["content"]["dirs"],
        })
        return urllib.urlencode(data)

    def update(self):
        status = self.data["stage"]["status"]
        desc = self.data["stage"]["error"]
        create_time = str(self.data["stage"]["created_ts"])
        if not self.update_info:
            return
        for obj_id, collection_name in self.update_info:
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
                    tb_name = tmp_col.find_one({"_id": obj_id})["name"]
                except Exception:
                    tb_name = ""
                tmp_task_id = re.split("_", self.task_id)
                tmp_task_id.pop()
                tmp_task_id.pop()
                insert_data = {
                    "table_id": obj_id,
                    "table_name": tb_name,
                    "task_id": "_".join(tmp_task_id),
                    "type_name": collection_name,
                    "status": "start",
                    "is_new": "new",
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                collection.insert_one(insert_data)
            elif status == "end":
                tmp_col = self.mongodb[collection_name]
                my_params = tmp_col.find_one({"_id": obj_id})["params"]
                my_dict = json.loads(my_params)
                if "submit_location" in my_dict:
                    insert_data = {
                        "status": status,
                        "desc": desc,
                        "params": my_params,
                        "submit_location": my_dict["submit_location"],
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                else:
                    insert_data = {
                        "status": status,
                        "desc": desc,
                        "params": my_params,
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                collection.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                               {'$set': insert_data}, upsert=True)
                super(UpdateStatus, self).update()
            else:
                insert_data = {
                    "status": status,
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                tmp_col = self.mongodb[collection_name]
                find_one = tmp_col.find_one({"_id": obj_id})
                if 'params' in find_one:
                    insert_data['params'] = find_one['params']
                if find_one['params']:
                    my_dict = json.loads(find_one['params'])
                    if "submit_location" in my_dict:
                        insert_data['submit_location'] = my_dict['submit_location']
                collection.find_one_and_update({"table_id": obj_id, "type_name": dbname}, {'$set': insert_data}, upsert=True)
            self._mongo_client.close()

