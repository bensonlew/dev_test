# -*- coding: utf-8 -*-
# __author__ = 'xuting'

import os
import json
from bson.objectid import ObjectId
from types import StringTypes
from biocluster.config import Config
from biocluster.api.database.base import Base, report_check


class SampleExtract(Base):
    def __init__(self, bind_object):
        super(SampleExtract, self).__init__(bind_object)
        self._db_name = Config().MONGODB

    @report_check
    def update_sg_seq_sample(self, list_path, table_id):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        file_sample = list()
        with open(list_path, "rb") as r:
            for line in r:
                if "#" in line:
                    continue
                line = line.rstrip().split("\t")
                file_sample.append({os.path.basename(line[0]): line[1]})

        collection = self.db["sg_seq_sample"]
        results = collection.find_one({"_id": table_id})
        if not results:
            raise Exception("table_id:{}在sg_seq_sample表里未找到".format(table_id))
        results["sample_info"] = json.dumps(file_sample)
        try:
            collection.find_one_and_update({"_id": table_id}, {'$set': results})
            self.bind_object.logger.info("表格导入成功")
        except Exception as e:
            self.bind_object.logger.error("表格导入出错:{}".format(e))
