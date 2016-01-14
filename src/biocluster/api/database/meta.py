# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from .base import Base
import os
import re
from bson.objectid import ObjectId
import datetime


class Meta(Base):
    def __init__(self, bind_object):
        super(Meta, self).__init__(bind_object)
        self._db_name = "sanger"

    def add_otu_table(self, file_path, level, from_out_table=0):
        if self.bind_object.IMPORT_REPORT_DATA is not True:
            self.bind_object.logger.debug("非web客户端调用，跳过导入OTU表格!")
            return
        if not os.path.isfile(file_path):
            raise Exception("文件%s不存在!" % file_path)
        if level not in ["kingdom", "phylum", "class", "order", "family", "genus", "species", "otu"]:
            raise Exception("level参数%s为不在允许范围内!" % level)
        if from_out_table != 0 and not isinstance(from_out_table, ObjectId):
            raise Exception("from_out_table必须为ObjectId对象!")

        data_list = []
        with open(file_path, 'r') as f:
            l = f.readline()
            if not re.match(r"^OTU ID", l):
                raise Exception("文件%s格式不正确，请选择正确的OTU表格文件" % file_path)
            sample_list = l.split("\t")
            sample_list.pop()
            sample_list.pop(0)
            insert_data = {
                "project_sn": self.bind_object.sheet.project_sn,
                "task_id": self.bind_object.sheet.id,
                "name": "原始表",
                "from_id": from_out_table,
                "level": level,
                "created_ts": datetime.datetime.now()
            }
            collection = self.db["sg_otu"]
            inserted_id = collection.insert_one(insert_data).inserted_id

            while True:
                line = f.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                classify = line_data.pop()
                classify_list = re.split(r"\s*;\s*", classify)
                data = {
                  "task_id": self.bind_object.sheet.id,
                  "otu_id": inserted_id,
                }
                for cf in classify_list:
                    if cf != "":
                        data[cf[0:3]] = cf
                i = 0
                for sample in sample_list:
                    i += 1
                    data[sample] = line_data[i]

                data_list.append(data)
        try:
            collection = self.db["sg_otu_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入OTU表格%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入OTU表格%s信息成功!" % file_path)
