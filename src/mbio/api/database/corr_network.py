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


class CorrNetwork(Base):
    def __init__(self, bind_object):
        super(CorrNetwork, self).__init__(bind_object)
        self._db_name = Config().MONGODB

    # @report_check
    def add_network_attributes(self, file_path, table_id = None, group_id = None, from_otu_table = None, level_id = None, major = False):
        if major:
            table_id = self.create_corrnetwork(self, params, group_id, from_otu_table, level_id)
        else:
            if not isinstance(table_id, ObjectId):
                if isinstance(table_id, StringTypes):
                    table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或者其对应的字符串！")
        data_list = []
        with open(file_path, "rb") as r:
            data = r.readlines()[1:]
            for line in data:
                line = line.strip().split('\t')
            data = [("corr_network_id", table_id), ("transitivity", eval(line[0])),
                    ("diameter", str(line[1])), ("average_shortest_path_length", str(line[2]))]
            data_son = SON(data)
            data_list.append(data_son)
        try:
            collection = self.db["sg_corr_network_structure_attributes"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list, table_id

    # @report_check
    def add_network_degree_distribution(self, file_path, table_id = None, group_id = None, from_otu_table = None, level_id = None, major = False):
        if major:
            table_id = self.create_corrnetwork(self, params, group_id, from_otu_table, level_id)
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
            data = r.readlines()[1:]
            for line in data:
                line = line.strip().split("\t")
                data = [("corr_network_id", table_id), ("degree", line[0]), ("num", line[1])]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["sg_corr_network_distribution_node"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    # @report_check
    def add_network_centrality(self, file_path, table_id = None, group_id = None, from_otu_table = None, level_id = None, major = False):
        if major:
            table_id = self.create_corrnetwork(self, params, group_id, from_otu_table, level_id)
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
                    data = [("corr_network_id", table_id), ("node_id", eval(line_data[0])),
                            ("node_name", line_data[1]),("degree_centrality", eval(line_data[2])),
                            ("closeness_centrality", eval(line_data[3])), ("betweenness_centrality", eval(line_data[4]))]
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_corr_network_centrality_node"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    # @report_check
    def add_network_cluster_degree(self,  file1_path, file2_path, params=None, group_id=None, from_otu_table=None, table_id=None, major=False):
        #file1_path:节点的degree表，file2_path:节点的cluster表
        if major:
            table_id = self.create_corrnetwork(self, params, group_id, from_otu_table, level_id)
        else:
            if table_id is None:
                raise Exception("major为False时需提供table_id!")
            if not isinstance(table_id, ObjectId):
                if isinstance(table_id, StringTypes):
                    table_id = ObjectId(table_id)
                else:
                    raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file1_path, 'rb') as r, open(file2_path, 'rb') as w:
            data1 = r.readlines()[1:]
            data2 = w.readlines()[1:]
            for line2 in data2:
                temp2 = line2.strip().split("\t")
                for line1 in data1:
                    temp1 = line1.strip().split("\t")
                    if temp1[1] == temp2[1]:
                        data = [("corr_network_id", table_id), ("node_id", eval(temp1[0])), ("node_name", temp1[1]), ("degree", eval(temp1[2])),
                                ("clustering", eval(temp2[2]))]
                        data_son = SON(data)
                        data_list.append(data_son)
        try:
            collection = self.db["sg_corr_network_structure_node"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file1_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file1_path)
        return data_list

    #@report_check
    def add_network_links_table(self, file_path, table_id=None, group_id=None, from_otu_table=None, level_id=None,
                               major=False):
        if major:
            table_id = self.create_corrnetwork(self, params, group_id, from_otu_table, level_id)
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
            data1 = r.readlines()[3:]
            for line in data1:
                line_data = line.strip().split('\t')
                data = [("corr_network_id", table_id), ("source", line_data[0]), ("target", line_data[1]),
                        ("value", eval(line_data[2]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["sg_corr_network_structure_link"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    # @report_check
    def add_network_abundance_table(self, file_path, table_id=None, group_id=None, from_otu_table=None, level_id=None,
                               major=False):
        if major:
            table_id = self.create_corrnetwork(self, params, group_id, from_otu_table, level_id)
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
            data = r.readlines()[1:]
            for line in data:
                line = line.strip().split("\t")
                data = [("corr_network_id", table_id), ("node_name", line[0]), ("abundance", eval(line[1]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["sg_corr_network_structure_abundance"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.error("导入%s信息成功!" % file_path)
        return data_list

    #@report_check
    def create_corrnetwork(self, params, group_id=0, from_otu_table=0, name=None, level_id=0):
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
        desc = "corrnetwork分析"
        insert_data = {
            "project_sn": project_sn,
            "task_id": task_id,
            "otu_id": from_otu_table,
            "group_id": group_id,
            "name": name if name else "corrnetwork_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            "params": params,
            "level_id": level_id,
            "desc": desc,
            "status": "end",
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_corr_network"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

