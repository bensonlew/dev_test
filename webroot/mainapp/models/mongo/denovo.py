# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
import types
from biocluster.config import Config
from mainapp.models.workflow import Workflow
import random


class Denovo(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db_name = Config().MONGODB + '_rna'
        self.db = self.client[self.db_name]

    def get_new_id(self, task_id, main_id):
        new_id = "%s_%s_%s" % (task_id, main_id[-4:], random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, main_id)
        return new_id

    def get_main_info(self, main_id, collection_name):
        if isinstance(main_id, types.StringTypes):
            main_id = ObjectId(main_id)
        elif isinstance(main_id, ObjectId):
            main_id = main_id
        else:
            raise Exception("输入main_id参数必须为字符串或者ObjectId类型!")
        collection = self.db[collection_name]
        express_info = collection.find_one({'_id': main_id})
        return express_info

    def get_task_info(self, task_id):
        sg_task = self.db['sg_task']
        result = sg_task.find_one({'task_id': task_id})
        return result
