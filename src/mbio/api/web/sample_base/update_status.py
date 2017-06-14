# -*- coding: utf-8 -*-
# __author__ = 'shijin'
from biocluster.config import Config
from mbio.api.web.meta.update_status import UpdateStatus as US
from biocluster.core.function import CJsonEncoder, filter_error_info
from bson.objectid import ObjectId
import json
import datetime


class UpdateStatus(US):

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._config = Config()
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://www.sanger.com/api/add_file"
        self._post_data = "%s&%s" % (self.get_sig(), self.get_post_data())
        self._mongo_client = self._config.mongo_client
        self.mongodb = self._mongo_client["samplebase"]

    def update_status(self):
        status = self.data["content"]["stage"]["status"]
        desc = filter_error_info(self.data["content"]["stage"]["error"])
        if not self.update_info:
            return
        for obj_id, collection_name in json.loads(self.update_info).items():
            obj_id = ObjectId(obj_id)
            collection = self.mongodb[collection_name]
            if status == "start":
                insert_data = {
                    "status": 'start',
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                collection.find_one_and_update({"table_id": obj_id}, {'$set': insert_data}, upsert=True)
            elif status == "finish":  # 只能有一次finish状态
                insert_data = {
                    "status": 'end',
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                collection.find_one_and_update({"table_id": obj_id}, {'$set': insert_data}, upsert=True)
            else:
                insert_data = {
                    "status": status,
                    "desc": desc,
                    "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                collection.find_one_and_update({"table_id": obj_id}, {'$set': insert_data}, upsert=True)
