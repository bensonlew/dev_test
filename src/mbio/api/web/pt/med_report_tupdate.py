# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
from pymongo import MongoClient
import json
import datetime
import re
from bson.objectid import ObjectId
from biocluster.wpm.log import Log
from biocluster.config import Config
from biocluster.core.function import CJsonEncoder, filter_error_info
from mbio.api.web.meta.update_status import UpdateStatus


class MedReportTupdate(UpdateStatus):

    def __init__(self, data):
        super(MedReportTupdate, self).__init__(data)
        self._config = Config()
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        # self.update_info = self.data["content"]["update_info"] if "update_info" in self.data["content"].keys() else None
        self._url = "http://www.tsanger.com/api/add_file"
        self._post_data = "%s&%s" % (self.get_sig(), self.get_post_data())
        # self._mongo_client = MongoClient(Config().MONGO_URI)
        # self.database = self._mongo_client['tsanger_paternity_test_v2']
        self._mongo_client = self._config.mongo_client
        self.database = self._mongo_client[Config().MONGODB+'_paternity_test_v2']

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
            collection = self.database[collection_name]
            if status != "start":
                data = {
                    "status": "end" if status == 'finish' else status,
                    "desc": desc,
                    "created_ts": create_time
                }
                collection.update({"_id": obj_id}, {'$set': data}, upsert=True)
            sg_status_col = self.database['sg_pt_father']
            if status == "start":
                tmp_col = self.database[collection_name]
                try:
                    temp_find = tmp_col.find_one({"_id": obj_id})
                    tb_name = temp_find["name"]
                    temp_params = temp_find['params']
                    submit_location = json.loads(temp_params)['submit_location']
                except:
                    tb_name = ""
                    temp_params = ''
                    submit_location = ''
                insert_data = {
                    "status": "start",
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                sg_status_col.update({"_id": obj_id}, {'$set': insert_data}, upsert=True)
            elif status == "finish":  # 只能有一次finish状态
                insert_data = {
                    "status": 'end',
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                sg_status_col.update({"_id": obj_id},
                                                  {'$set': insert_data}, upsert=True)
            else:
                insert_data = {
                    "status": status,
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                sg_status_col.update({"_id": obj_id},
                                                  {'$set': insert_data}, upsert=True)
            self._mongo_client.close()
