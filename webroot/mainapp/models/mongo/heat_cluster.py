# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from bson.objectid import ObjectId
import datetime
from types import StringTypes
from mainapp.config.db import get_mongo_client
import re


class HeatCluster(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client["sanger"]

    def create_newick_table(self, params, linkage, from_otu_table=0, name=None):
        if from_otu_table != 0 and not isinstance(from_otu_table, ObjectId):
            if isinstance(from_otu_table, StringTypes):
                from_otu_table = ObjectId(from_otu_table)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_otu"]
        result = collection.find_one({"_id": from_otu_table})
        if not result:
            raise Exception("无法根据传入的_id:{}在sg_otu表里找到相应的记录".format(str(from_otu_table)))
        project_sn = result['project_sn']
        task_id = result['task_id']
        desc = "正在计算聚类树..."
        insert_data = {
            "project_sn": project_sn,
            "task_id": task_id,
            "table_type": "dist",
            "tree_type": "cluster",
            "hcluster_method": linkage,
            "status": "start",
            "desc": desc,
            "name": name if name else "cluster_newick",
            "params": params,
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db['sg_newick_tree']
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    def sample_id_to_name(self, sample_ids):
        my_ids = re.split(",", sample_ids)
        collection = self.db["sg_specimen"]
        my_sample_names = list()
        for id_ in my_ids:
            if id_ == "":
                raise Exception("存在空的sample_id")
            if not isinstance(id_, ObjectId):
                if isinstance(id_, StringTypes):
                    id_ = ObjectId(id_)
                else:
                    raise Exception("样本id必须为ObjectId对象或其对应的字符串!")
            result = collection.find_one({"_id": id_})
            if not result:
                raise Exception("无法根据传入的_id:{}在sg_speciem表里找到相应的记录".format(str(id_)))
            my_sample_names.append(result["specimen_name"])
        my_sample = ",".join(my_sample_names)
        return my_sample
