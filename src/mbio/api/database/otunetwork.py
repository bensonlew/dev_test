# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes
from bson.son import SON
import gridfs
import datetime
import os
from biocluster.config import Config


class Otunetwork(Base):
    def __init__(self, bind_object):
        super(Otunetwork, self).__init__(bind_object)
        self._db_name = Config().MONGODB

    # @report_check
    def add_network_attributes(self, file_path, table_id = None, group_id = None, from_otu_table = None, level_id = None, major = False):
        if major:
            table_id = self.create_otunetwork(self, params, group_id, from_otu_table, level_id)
        else:
            if not isinstance(table_id, ObjectId):
                if isinstance(table_id, StringTypes):
                    table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或者其对应的字符串！")
        data_list = []
        data_list1 = []
        with open(file_path, "rb") as r:
            for line in r:
                line = line.strip('\n')
                line_data = line.split('\t')
                data_list1.append(line_data)
            data = [("network_id", table_id), ("Transitivity", eval(data_list1[0][1])),
                    ("Diameter", eval(data_list1[1][1])), ("Average_shortest_path_length", eval(data_list1[2][1]))]
            data_son = SON(data)
            data_list.append(data_son)
        try:
            collection = self.db["sg_meta_network_attributes"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list, table_id

    # @report_check
    def add_network_degree(self, file_path, table_id = None, group_id = None, from_otu_table = None, level_id = None, major = False):
        if major:
            table_id = self.create_otunetwork(self, params, group_id, from_otu_table, level_id)
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
                    data = [("network_id", table_id), ("OTU_Degree", line_data[0]), ("OTU_Num", line_data[1]), ("Sample_Degree", line_data[2]),
                            ("Sample_Num", line_data[3]), ("Node_Degree", line_data[4]), ("Node_Num", line_data[5])]
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_meta_network_degree"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    # @report_check
    def add_network_centrality(self, file_path, table_id = None, group_id = None, from_otu_table = None, level_id = None, major = False):
        if major:
            table_id = self.create_otunetwork(self, params, group_id, from_otu_table, level_id)
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
                    data = [("network_id", table_id), ("Node_ID", eval(line_data[0])),
                            ("Node_Name", line_data[1]),("Degree_Centrality", eval(line_data[2])),
                            ("Closeness_Centrality", eval(line_data[3])), ("Betweenness_Centrality", eval(line_data[4]))]
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_meta_network_centrality"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    # @report_check
    def add_node_table(self,  file_path, params=None, group_id=None, from_otu_table=None, table_id=None, major=False):
        if major:
            table_id = self.create_otunetwork(self, params, group_id, from_otu_table, level_id)
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
            data_line = r.readlines()[1:]
            for line in data_line:
                line_data = line.strip().split('\t')
                match_obj = re.match('d__', line_data[0])
                if match_obj is not None:
                    line1 = line_data[0].strip().split(';')
                    data = [("network_id", table_id), ("node_name", line_data[0]), ("node_disp_name", line1[len(line1)-1]),
                            ("ntype", line_data[1]), ("degree", eval(line_data[2])), ("weighted_degree", " ")]
                else:
                    data = [("network_id", table_id), ("node_name", line_data[0]), ("node_disp_name", line_data[1]),
                            ("ntype", line_data[2]), ("degree", eval(line_data[3])), ("weighted_degree", float(line_data[4]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["sg_meta_network_node_table"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    # @report_check
    def add_node_table_group(self, file_path, params=None, group_id=None, from_otu_table=None, table_id=None, major=False):
        #该步是用于添加后面画网络图的时候，节点进行分组显示，传入的数据是workflow生成文件是real_node_table.txt。
        if major:
            table_id = self.create_otunetwork(self, params, group_id, from_otu_table, level_id)
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
            data_line = r.readlines()[1:]
            for line in data_line:
                line_data = line.strip().split('\t')
                match_obj = re.match('d__', line_data[0])
                if match_obj is not None:
                    line1 = line_data[0].strip().split(';')
                    line2 = line1[len(line1)-1]
                    data = [("network_id", table_id), ("id", line2),
                            ("group", line2[0:3])]
                else:
                    line1 = line_data[0].strip().split("_")
                    l = len(line1)
                    line2 = line1[0:l-1]
                    line3 = "_".join(line2)
                    data = [("network_id", table_id), ("id", line_data[0]),
                            ("group", line3)]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["sg_meta_network_node_table_group"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    #@report_check
    def add_edge_table(self, file_path, table_id=None, group_id=None, from_otu_table=None, level_id=None,
                               major=False):
        if major:
            table_id = self.create_otunetwork(self, params, group_id, from_otu_table, level_id)
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
            data1 = r.readlines()[1:]
            for line in data1:
                line_data = line.strip().split('\t')
                line_data1 = line_data[1].rstrip().split(";")
                data = [("network_id", table_id), ("source", line_data[0]), ("target", line_data1[len(line_data1)-1]),
                        ("value", eval(line_data[2]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["sg_meta_network_link"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    #@report_check
    def create_otunetwork(self, params, group_id=0, from_otu_table=0, name=None, level_id=0):
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

        collection = self.db["sg_otu"]
        result = collection.find_one({"_id": from_otu_table})
        project_sn = result['project_sn']
        task_id = result['task_id']
        desc = "otunetwork分析"
        insert_data = {
            "project_sn": project_sn,
            "task_id": task_id,
            "otu_id": from_otu_table,
            "group_id": group_id,
            "name": name if name else "otunetwork_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            "params": params,
            "level_id": level_id,
            "desc": desc,
            "status": "end",
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_meta_otunetwork"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

