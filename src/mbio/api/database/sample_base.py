# -*- coding: utf-8 -*-
# __author__ = 'shijin'

import os
import json
from bson.objectid import ObjectId
from types import StringTypes
from biocluster.config import Config
from biocluster.api.database.base import Base, report_check


class SampleBase(Base):
    def __init__(self, bind_object):
        super(SampleBase, self).__init__(bind_object)
        self._db_name = "samplebase"

    @report_check
    def add_sg_test_specimen(self, sample, stat_path, file_sample, table_id):
        """
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
                """
        collection = self.db["sg_test_batch_specimen"]
        results = {}
        results["test_batch_id"] = ObjectId(table_id)
        with open(stat_path, "r") as file:
            for line in file:
                if line.startswith(sample):
                    tmp = line.strip().split("\t")
                    results["specimen_name"] = tmp[0]
                    results["total_reads"] = tmp[1]
                    results["total_bases"] = tmp[2]
                    results["total_reads_with_ns"] = tmp[3]
                    results["n_reads%"] = tmp[4]
                    results["A%"] = tmp[6]
                    results["T%"] = tmp[7]
                    results["C%"] = tmp[8]
                    results["G%"] = tmp[9]
                    results["N%"] = tmp[10]
                    results["Error%"] = tmp[11]
                    results["Q20%"] = tmp[12]
                    results["Q30"] = tmp[13]
                    results["GC"] = tmp[14]
        results["path"] = []
        for key in file_sample.keys():
            if file_sample[key] == sample:
                results["path"].append(key)
        try:
            sample_id = collection.insert_one(results).insert_id
            self.bind_object.logger.info("表格导入成功")
        except Exception as e:
            self.bind_object.logger.error("表格导入出错:{}".format(e))
        return sample_id

    @report_check
    def add_sg_test_batch_specimen(self, table_id, sample_id, sample):
        collection = self.db["sg_test_batch_specimen"]
        results = {}
        results["test_batch_id"] = ObjectId(table_id)
        results["test_specimen_id"] = ObjectId(sample_id)
        results["alias_name"] = sample
        try:
            collection.insert_one(results)
            self.bind_object.logger.info("表格导入成功")
        except Exception as e:
            self.bind_object.logger.error("表格导入出错:{}".format(e))

    @report_check
    def add_sg_test_batch_task_specimen(self, table_id, sample_id, sample):
