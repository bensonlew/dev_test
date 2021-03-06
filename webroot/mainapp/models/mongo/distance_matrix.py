# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
import types
from biocluster.config import Config


class Distance(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client[Config().MONGODB]

    def get_distance_matrix_info(self, distance_id):

        if isinstance(distance_id, types.StringTypes):
            distance_id = ObjectId(distance_id)
        elif isinstance(distance_id, ObjectId):
            pass
        else:
            raise Exception("输入distance_id参数必须为字符串或者ObjectId类型!")
        collection = self.db['sg_beta_specimen_distance']
        result = collection.find_one({"_id": distance_id})
        return result
