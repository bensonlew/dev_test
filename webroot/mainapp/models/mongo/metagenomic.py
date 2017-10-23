# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
import types
from bson import SON
from biocluster.config import Config
from mainapp.models.workflow import Workflow
from mainapp.models.mongo.meta import Meta
import random
import json

class Metagenomic(Meta):
    def __init__(self):
        self.db_name = Config().MONGODB + '_metagenomic'
        super(Metagenomic, self).__init__(db=self.db_name)
        
    def get_geneset_info(self, geneset_id):
        if isinstance(geneset_id, types.StringTypes):
            geneset_id = ObjectId(geneset_id)
        elif isinstance(geneset_id, ObjectId):
            geneset_id = geneset_id
        else:
            raise Exception("输入geneset_id参数必须为字符串或者ObjectId类型!")
        collection = self.db['geneset']
        result = collection.find_one({"_id": geneset_id})
        return result