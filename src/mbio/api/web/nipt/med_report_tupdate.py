# -*- coding: utf-8 -*-
# __author__ = 'hongdong.xuan'
import json
import datetime
from bson.objectid import ObjectId
from biocluster.config import Config
from biocluster.core.function import filter_error_info
# from mbio.api.web.meta.update_status import UpdateStatus
from ..meta.update_status import UpdateStatus


class MedReportTupdate(UpdateStatus):

    def __init__(self, data):
        super(MedReportTupdate, self).__init__(data)
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        self._url = "http://api.tsg.com/task/add_file"
        # self._mongo_client = self._config.mongo_client
        self.database = self._mongo_client[self.config.MONGODB + '_nipt']

    def update(self, web_api=False):
        super(MedReportTupdate, self).update(web_api=False)

    def update_status(self):
        status = self.data["sync_task_log"]["task"]["status"]
        self.logger.info("status %s" % (status))
        desc = ''
        for i in self.data['sync_task_log']['log']:
            if 'name' not in i:
                desc = i['desc']
        desc = filter_error_info(desc)
        create_time = str(self.data["sync_task_log"]["task"]["created_ts"])

        if not self.update_info:
            return

        for obj_id, collection_name in self.update_info.items():
            obj_id = ObjectId(obj_id)
            collection = self.database[collection_name]
            if status != "start":
                data = {
                    "status": "end" if status == 'finish' else status,
                    "desc": desc,
                    "time": create_time
                }
                collection.update({"_id": obj_id}, {'$set': data}, upsert=True)
            # sg_status_col = self.database[collection_name]
            # if status == "start":
            #     tmp_col = self.database[collection_name]
            #     try:
            #         temp_find = tmp_col.find_one({"_id": obj_id})
            #         tb_name = temp_find["name"]
            #         temp_params = temp_find['params']
            #         submit_location = json.loads(temp_params)['submit_location']
            #     except:
            #         tb_name = ""
            #         temp_params = ''
            #         submit_location = ''
            #     insert_data = {
            #         "status": "start",
            #         "desc": desc,
            #         "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #     }
            #     sg_status_col.update({"_id": obj_id}, {'$set': insert_data}, upsert=True)
            # elif status == "finish":  # 只能有一次finish状态
            #     insert_data = {
            #         "status": 'end',
            #         "desc": desc,
            #         "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #     }
            #     sg_status_col.update({"_id": obj_id}, {'$set': insert_data}, upsert=True)
            # else:
            #     insert_data = {
            #         "status": status,
            #         "desc": desc,
            #         "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #     }
            #     sg_status_col.update({"_id": obj_id}, {'$set': insert_data}, upsert=True)
            # self._mongo_client.close()
