# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
# last_modified = shenghe
import urllib
import json
import datetime
import re
import sys
from bson.objectid import ObjectId
from biocluster.wpm.log import Log
from biocluster.core.function import CJsonEncoder, filter_error_info
import traceback


class UpdateStatus(Log):
    """
    meta的web api，用于更新sg_status表并向前端发送状态信息和文件上传信息
    一般可web api功能可从此处继承使用，需要重写__init__方法
    """

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://api.sanger.com/task/add_file"
        self._mongo_client = self.config.mongo_client
        self.mongodb = self._mongo_client[self.config.MONGODB + '_metagenomic']

    def __del__(self):
        self._mongo_client.close()

    @property
    def post_data(self):
        workflow_id = self.data["sync_task_log"]['task']["task_id"]
        my_id = re.split('_', workflow_id)
        my_id.pop(-1)
        my_id.pop(-1)
        data = dict()
        content = {
            "task": {
                "task_id": "_".join(my_id)
            }
        }
        if 'files' in self.data['sync_task_log'].keys():
            content['files'] = self.data["sync_task_log"]["files"]
        if 'dirs' in self.data['sync_task_log'].keys():
            content['dirs'] = self.data['sync_task_log']['dirs']
        if 'base_path' in self.data['sync_task_log'].keys():
            content['base_path'] = self.data['sync_task_log']['base_path']
        data['sync_task_log'] = json.dumps(content, cls=CJsonEncoder)
        return urllib.urlencode(data)

    def update(self):
        if 'files' in self.data['sync_task_log'].keys() or 'dirs' in self.data['sync_task_log'].keys():
            self.send()

    def update_status(self):
        status = self.data["sync_task_log"]["task"]["status"]
        desc = ''
        for i in self.data['sync_task_log']['log']:
            if 'name' not in i:
                desc = i['desc']
        desc = filter_error_info(desc)
        create_time = str(self.data["sync_task_log"]["task"]["created_ts"])
        if not self.update_info:
            return
        batch_id = self.update_info.pop("batch_id") if 'batch_id' in self.update_info else None
        for obj_id, collection_name in self.update_info.items():
            obj_id = ObjectId(obj_id)
            collection = self.mongodb[collection_name]
            if status != "start":
                data = {
                    "status": "end" if status == 'finish' else status,
                    "desc": desc,
                    "created_ts": create_time
                }
                collection.find_one_and_update({"_id": obj_id}, {'$set': data}, upsert=True)
            sg_status_col = self.mongodb['sg_status']
            if status == "start":
                if not batch_id:
                    tmp_col = self.mongodb[collection_name]
                    try:
                        temp_find = tmp_col.find_one({"_id": obj_id})
                        tb_name = temp_find["name"]
                        temp_params = temp_find['params']
                        submit_location = json.loads(temp_params)['submit_location']
                    except Exception, e:
                        exstr = traceback.format_exc()
                        print exstr
                        print e
                        sys.stdout.flush()
                        tb_name = ""
                        temp_params = ''
                        submit_location = ''
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
                if not batch_id:
                    insert_data = {
                        "status": 'end',
                        "desc": desc,
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    sg_status_col.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                                      {'$set': insert_data}, upsert=True)
                # self.pipe_update(batch_id, collection_name, obj_id, "end", desc)
            else:
                if not batch_id:
                    insert_data = {
                        "status": status,
                        "desc": desc,
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    sg_status_col.find_one_and_update({"table_id": obj_id, "type_name": collection_name},
                                                      {'$set': insert_data}, upsert=True)
                # self.pipe_update(batch_id, collection_name, obj_id, status, desc)
