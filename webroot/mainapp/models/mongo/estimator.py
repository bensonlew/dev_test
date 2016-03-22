# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from bson.objectid import ObjectId
import datetime
from types import StringTypes
from mainapp.config.db import get_mongo_client
import types


class Estimator(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client["sanger"]

    def add_est_collection(self, level, params, from_otu_table=0, name=None):
        if int(level) not in range(1, 10):
            raise Exception("level参数%s为不在允许范围内!" % level)
        if from_otu_table != 0 and not isinstance(from_otu_table, ObjectId):
            if isinstance(from_otu_table, StringTypes):
                from_otu_table = ObjectId(from_otu_table)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_otu"]
        result = collection.find_one({"_id": from_otu_table})
        project_sn = result['project_sn']
        task_id = result['task_id']
        insert_data = {
                "project_sn": project_sn,
                "task_id": task_id,
                "otu_id": from_otu_table,
                "name": name if name else "estimators_origin",
                "level_id": int(level),
                "status": "start",
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        collection = self.db["sg_alpha_diversity"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    def add_est_t_test_collection(self, params, group_id, from_est_id=0, name=None):
        if isinstance(from_est_id, StringTypes):
            from_est_id = ObjectId(from_est_id)
        else:
            raise Exception("est_id必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_alpha_diversity"]
        result = collection.find_one({"_id": from_est_id})
        project_sn = result['project_sn']
        task_id = result['task_id']
        otu_id = result['otu_id']
        level_id = result['level_id']
        insert_data = {
                "project_sn": project_sn,
                "task_id": task_id,
                "otu_id": otu_id,
                "alpha_diversity_id": from_est_id,
                "name": name if name else "多样性指数T检验结果表",
                "level_id": int(level_id),
                "group_id": group_id,
                "status": "start",
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        collection = self.db["sg_alpha_est_t_test"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    def get_est_table_info(self, est_id):
        if isinstance(est_id, types.StringTypes):
            est_id = ObjectId(est_id)
        elif isinstance(est_id, ObjectId):
            est_id = est_id
        else:
            raise Exception("输入est_id参数必须为字符串或者ObjectId类型!")
        collection = self.db['sg_alpha_diversity']
        result = collection.find_one({"_id": est_id})
        return result

    def add_rare_collection(self, level, params, from_otu_table=0, name=None):
        if int(level) not in range(1, 10):
            raise Exception("level参数%s为不在允许范围内!" % level)
        if from_otu_table != 0 and not isinstance(from_otu_table, ObjectId):
            if isinstance(from_otu_table, StringTypes):
                from_otu_table = ObjectId(from_otu_table)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_otu"]
        result = collection.find_one({"_id": from_otu_table})
        project_sn = result['project_sn']
        task_id = result['task_id']
        insert_data = {
                "project_sn": project_sn,
                "task_id": task_id,
                "otu_id": from_otu_table,
                "name": name if name else "rarefaction_origin",
                "level_id": int(level),
                "status": "start",
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        collection = self.db["sg_alpha_rarefaction_curve"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id
