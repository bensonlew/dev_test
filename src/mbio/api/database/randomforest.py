#-*- coding: utf-8 -*-
# __author__ = 'zhangpeng'
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes
from bson.son import SON
import gridfs
import datetime
import os
# from biocluster.config import Config


class Randomforest(Base):
    def __init__(self, bind_object):
        super(Randomforest, self).__init__(bind_object)
        self._project_type = 'meta'
        # self._db_name = Config().MONGODB

    @report_check
    def add_randomforest_error(self, file_path, table_id = None, group_id = None, from_otu_table = None, level_id = None, major = False):
        self.bind_object.logger.info('start insert mongo zhangpeng')
        if major:
            table_id = self.create_randomforest(self, params, group_id, from_otu_table, level_id)
        else:
            if not isinstance(table_id, ObjectId):
                if isinstance(table_id, StringTypes):
                    table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或者其对应的字符串！")
        data_list = []
        data_list1 = []
        b1 = len(file_path)
        b2 = len(file_path[0])
        with open(file_path, "rb") as r:
            for line in r:
                line = line.strip('\n')
                line_data = line.split('\t')
                data_list1.append(line_data)
            data = [("randomforest_id", table_id), ("class.error", line_data[b2+1])]
            data_son = SON(data)
            data_list.append(data_son)
        try:
            collection = self.db["sg_randomforest_specimen_table"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list, table_id

    # @report_check
    def add_randomforest_dim(self, file_path, table_id = None, group_id = None, from_otu_table = None, level_id = None, major = False):
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
                    data = [("randomforest_id", table_id),("specimen_name", line_data[0]),("x", line_data[1]), ("y", line_data[2])]
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_randomforest_specimen_scatter"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list


    # @report_check
    def add_randomforest_vip(self, file_path, table_id = None, group_id = None, from_otu_table = None, level_id = None, major = False):
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

        a1 = len(file_path)
        a2 = len(file_path[0])
        with open(file_path, 'rb') as r:
            i = 0
            for line in r:
                if i == 0:
                    i = 1
                else:
                    line = line.strip('\n')
                    line_data = line.split('\t')
                    #data = [("randomforest_id", table_id),("specimen_name","line_data[0]") ("accuracy", line_data[a2-1]), ("gini", line_data[a2])]
                    data = [("randomforest_id", table_id), ("species_name", line_data[0]), ("accuracy", float(line_data[-2])), ("gini", float(line_data[-1]))]
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_randomforest_species_bar"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list



    #@report_check
    def create_randomforest(self, params, group_id=0, from_otu_table=0, name=None, level_id=0):
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
        desc = "randomforest分析"
        insert_data = {
            "project_sn": project_sn,
            "task_id": task_id,
            "otu_id": from_otu_table,
            "group_id": group_id,
            "name": name if name else "oturandomforest_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            "params": params,
            "level_id": level_id,
            "desc": desc,
            "status": "end",
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_meta_randomforest"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id
