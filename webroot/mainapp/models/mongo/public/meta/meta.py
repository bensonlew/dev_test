# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from mainapp.config.db import get_mongo_client
from bson.objectid import ObjectId
import types
import re


class Meta(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client["sanger"]

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

    def sampleIdToName(self, sampleIds):
        """
        将一个用逗号隔开的样本ID的集合转换成样本名，返回一个用逗号隔开的样本名的集合
        """
        myIds = re.split("\s*,\s*", sampleIds)
        collection = self.db["sg_specimen"]
        mySampleNames = list()
        for id_ in myIds:
            if id_ == "":
                raise Exception("存在空的sample_id")
            if not isinstance(id_, ObjectId):
                if isinstance(id_, types.StringTypes):
                    id_ = ObjectId(id_)
                else:
                    raise Exception("样本id必须为ObjectId对象或者其对应的字符串！")
                result = collection.find_one({"_id": id_})
                if not result:
                    raise Exception("无法根据传入的_id:{}在sg_speciem表里找到相应的记录".format(str(id_)))
                mySampleNames.append(result["specimen_name"])
            mySamples = ",".join(mySampleNames)
            return mySamples
