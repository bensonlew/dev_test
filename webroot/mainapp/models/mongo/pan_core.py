# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from bson.objectid import ObjectId
import datetime
from types import StringTypes
from mainapp.config.db import get_mongo_client


class PanCore(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client["sanger"]

    def create_pan_core_table(self, pan_core_type, params, group_id, from_otu_table=0, name=None):
        if from_otu_table != 0 and not isinstance(from_otu_table, ObjectId):
            if isinstance(from_otu_table, StringTypes):
                from_otu_table = ObjectId(from_otu_table)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        if not isinstance(group_id, ObjectId):
            if isinstance(group_id, StringTypes):
                group_id = ObjectId(group_id)
            else:
                raise Exception("group_id必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_otu"]
        result = collection.find_one({"_id": from_otu_table})
        project_sn = result['project_sn']
        task_id = result['task_id']
        if pan_core_type == 1:
            desc = "正在计算pan otu表格"
        else:
            desc = "正在计算core otu表格"
        insert_data = {
            "type": pan_core_type,
            "project_sn": project_sn,
            "task_id": task_id,
            "group_id": group_id,
            "status": "start",
            "desc": desc,
            "name": name if name else "pan_core表格",
            "params": params,
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_otu_pan_core"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id
