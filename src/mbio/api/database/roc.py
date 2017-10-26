# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'
# last_modify: 20170516 zhouxuan to roc_new.py(workflow)
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes
from bson.son import SON
import gridfs
import datetime
import os
# from biocluster.config import Config


class Roc(Base):
    def __init__(self, bind_object):
        super(Roc, self).__init__(bind_object)
        self._project_type = 'meta'
        # self._db_name = Config().MONGODB

    @report_check
    def add_roc_curve(self, file_path, table_id=None, group_id=None,
                      from_otu_table=None, level_id=None, major=False, dir_name=None):
        # self.bind_object.logger.info('start insert mongo zhangpeng')
        if major:
            table_id = self.create_roc(self, params, group_id, from_otu_table, level_id)
        else:
            if not isinstance(table_id, ObjectId):
                if isinstance(table_id, StringTypes):
                    table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或者其对应的字符串！")
        data_list = []
        with open(file_path, 'rb') as r:
            i = 0
            for line in r:
                if i == 0:
                    i = 1
                else:
                    line = line.strip('\n')
                    line_data = line.split('\t')
                    if dir_name is None:
                        data = [("roc_id", table_id), ("x", line_data[0]), ("y", line_data[1])]
                    else:
                        data = [("roc_id", table_id), ("x", line_data[0]), ("y", line_data[1]),
                                ("analysis_name", dir_name)]
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_roc_curve"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list, table_id

    # @report_check
    def add_roc_auc(self, file_path, table_id=None, group_id=None,
                    from_otu_table=None, level_id=None, major=False, dir_name=None):
        if major:
            table_id = self.create_randomforest(self, params, group_id, from_otu_table, level_id)
        else:
            if table_id is None:
                raise Exception("major为False时需提供table_id!")
            if not isinstance(table_id, ObjectId):
                if isinstance(table_id, StringTypes):
                    table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            i = 0
            for line in r:
                if i == 0:
                    i = 1
                else:
                    line = line.strip('\n')
                    line_data = line.split('\t')
                    if dir_name is None:
                        data = [("roc_id", table_id), ("auc", line_data[0])]
                    else:
                        data = [("roc_id", table_id), ("auc", line_data[0]), ("analysis_name", dir_name)]
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_roc_auc"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    def add_roc_area(self, file_path=None, table_id=None, dir_name=None):
        """
        置信度区间信息导表 zhoxuan
        :param file_path:
        :param table_id:
        :param dir_name:
        :return: nothing
        """
        if table_id is None:
            raise Exception("major为False时需提供table_id!")
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
        else:
            raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            i = 0
            for line in r:
                if i == 0:
                    i = 1
                else:
                    line = line.strip('\n')
                    line_data = line.split('\t')
                    if dir_name is None:
                        data = [("roc_id", table_id), ("x", line_data[0]), ("y_min", line_data[1]),
                                ("y_max", line_data[2])]
                    else:
                        data = [("roc_id", table_id), ("x", line_data[0]), ("y_min", line_data[1]),
                                ("y_max", line_data[2]), ("analysis_name", dir_name)]
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_roc_area"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)








    #@report_check
    def create_roc(self, params, group_id=0, from_otu_table=0, name=None, level_id=0):
        if from_otu_table != 0 and not isinstance(from_otu_table, ObjectId):
            if isinstance(from_otu_table, StringTypes):
                from_otu_table = ObjectId(from_otu_table)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        if group_id != 0 and not isinstance(group_id, ObjectId):
            if isinstance(group_id, StringTypes):
                group_id = ObjectId(group_id)
            else:
                raise Exception("group_detail必须为ObjectId对象或其对应的字符串!")
        if level_id not in range(1, 10):
            raise Exception("level参数%s为不在允许范围内!" % level_id)

        collection = self.db["sg_otu"]  #我是不是也可以 用这个表
        result = collection.find_one({"_id": from_otu_table})
        project_sn = result['project_sn']
        task_id = result['task_id']
        desc = "roc分析"
        insert_data = {
            "project_sn": project_sn,
            "task_id": task_id,
            "otu_id": from_otu_table,
            "group_id": group_id,
            "name": name if name else "oturoc_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            "params": params,
            "level_id": level_id,
            "desc": desc,
            "status": "end",
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_meta_roc"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id
