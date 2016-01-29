# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

from bson.objectid import ObjectId
import datetime
from types import StringTypes
from mainapp.config.db import get_mongo_client


class GroupStat(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client["sanger"]

    def create_species_difference_check(self, level, check_type, params, group_id=0,  from_otu_table=0, name=None):
        if from_otu_table != 0 and not isinstance(from_otu_table, ObjectId):
            if isinstance(from_otu_table, StringTypes):
                from_otu_table = ObjectId(from_otu_table)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        if group_id != 0 and not isinstance(group_id, ObjectId):
            if isinstance(group_id, StringTypes):
                group_id = ObjectId(group_id)
            else:
                raise Exception("group_detail必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_otu"]
        result = collection.find_one({"_id": from_otu_table})
        project_sn = result['project_sn']
        task_id = result['task_id']
        if check_type == 'tow_sample':
             insert_data = {
                "type": check_type,
                "project_sn": project_sn,
                "task_id": task_id,
                "name": name if name else "组间差异统计表格",
                "level_id": level,
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
             }
        else:    
            insert_data = {
                "type": check_type,
                "project_sn": project_sn,
                "task_id": task_id,
                "otu_id": from_otu_table,
                "group_id": group_id,
                "name": name if name else "组间差异统计表格",
                "level_id": level,
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        collection = self.db["sg_species_difference_check"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id
