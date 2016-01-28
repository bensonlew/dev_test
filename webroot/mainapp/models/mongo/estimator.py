# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from bson.objectid import ObjectId
import datetime
from types import StringTypes
from mainapp.config.db import get_mongo_client


class Estimator(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client["sanger"]

    def add_est_collection(self, level, params, from_otu_table=0, name=None):
        if level not in range(1, 10):
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
                "level_name": level,
                # "status": "end",
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        collection = self.db["sg_alpha_diversity"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    def add_est_t_test_collection(self, params, est_id=0, name=None):
        if isinstance(est_id, StringTypes):
            est_id = ObjectId(est_id)
        else:
            raise Exception("est_id必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_alpha_est_t_test"]
        result = collection.find_one({"_id": est_id})
        project_sn = result['project_sn']
        task_id = result['task_id']
        otu_id = result['otu_id']
        level_id = result['level_id']
        insert_data = {
                "project_sn": project_sn,
                "task_id": task_id,
                "otu_id": otu_id,
                "name": name if name else "多样性指数T检验结果表",
                "level_id": level_id,
                # "status": "end",
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        collection = self.db["sg_alpha_est_t_test"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id
