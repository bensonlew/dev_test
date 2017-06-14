# -*- coding: utf-8 -*-
# __author__ = 'hongdong.xuan'
from bson import SON
from bson import ObjectId
from biocluster.config import Config
from types import StringTypes


class NiptMongo(object):
    '''
    获取流程的样本名
    '''

    def __init__(self):
        self.mongo_client = Config().mongo_client
        self.database = self.mongo_client[Config().MONGODB + '_nipt']

    def get_sample_info(self, _id):
        if not isinstance(_id, ObjectId):
            if isinstance(_id, StringTypes):
                _id = ObjectId(_id)
            else:
                raise Exception('_id必须为ObjectId对象或其对应的字符串!')
        collection = self.database['sg_nipt_main']
        task_info = collection.find_one({"_id": _id})
        return task_info

    def insert_main_table(self, collection, data):
        return self.database[collection].insert_one(SON(data)).inserted_id

    def insert_none_table(self, collection, data):
        return self.database[collection].insert_one({}).inserted_id


