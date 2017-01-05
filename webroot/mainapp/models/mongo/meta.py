# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
import types
from biocluster.config import Config
from bson import SON


class Meta(object):
    def __init__(self, db=None):
        self.client = get_mongo_client()
        if not db:
            self.db = self.client[Config().MONGODB]
        else:
            self.db = self.client[db]

    def get_otu_table_info(self, otu_id):

        if isinstance(otu_id, types.StringTypes):
            otu_id = ObjectId(otu_id)
        elif isinstance(otu_id, ObjectId):
            otu_id = otu_id
        else:
            raise Exception("输入otu_id参数必须为字符串或者ObjectId类型!")
        collection = self.db['sg_otu']
        result = collection.find_one({"_id": otu_id})
        return result

    def get_task_info(self, task_id):
        collection = self.db['sg_task']
        result = collection.find_one({"task_id": task_id})
        return result

    def insert_main_table(self, collection, data):
        return self.db[collection].insert_one(SON(data)).inserted_id
