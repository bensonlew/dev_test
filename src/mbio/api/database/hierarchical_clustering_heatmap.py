# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'

from biocluster.api.database.base import Base, report_check
import re
import datetime
# from bson.objectid import ObjectId
from types import StringTypes
from biocluster.config import Config


class HierarchicalClusteringHeatmap(Base):
    """
    聚类不聚类heatmap图合并api接口（因为分组求的问题这边的表不能再被利用）
    """
    def __init__(self, bind_object):
        super(HierarchicalClusteringHeatmap, self).__init__(bind_object) #
        self._project_type = 'meta'
        # self._db_name = Config().MONGODB
        self.task_id = ""
        self.name_id = dict()

    @report_check
    def add_sg_hc_heatmap(self, params, from_otu_table, name=None, sample_tree=None, sample_list=None, species_tree=None, species_list=None):
        if from_otu_table != 0 and not isinstance(from_otu_table, ObjectId):
            if isinstance(from_otu_table, StringTypes):
                from_otu_table = ObjectId(from_otu_table)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_otu"]
        result = collection.find_one({"_id": from_otu_table})
        if not result:
            raise Exception("无法根据传入的_id:{}在sg_otu表里找到相应的记录".format(str(from_otu_table)))
        project_sn = result['project_sn']
        self.task_id = result['task_id']
        if not name:
            name = "hierarchical_clustering_heatmap_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") #
        insert_data = {
            "project_sn": project_sn,
            'task_id': self.task_id,
            'otu_id': from_otu_table,
            'name': self.bind_object.sheet.main_table_name if self.bind_object.sheet.main_table_name else name,
            "params": params,
            "sample_tree": sample_tree,
            "sample_list": sample_list,
            "species_tree": species_tree,
            "species_list": species_list,
            'status': 'end',
            'desc': 'otu table after Hierarchical Clustering Heatmap', #
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "show": 0,
            "type": "otu_HierarchicalClusteringHeatmap" #
        }
        collection = self.db["sg_hc_heatmap"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    @report_check
    def add_sg_hc_heatmap_detail(self, file_path, new_otu_id, from_otu_id, sample_tree=None, sample_list=None, species_tree=None, species_list=None):
        if from_otu_id != 0 and not isinstance(from_otu_id, ObjectId):
            if isinstance(from_otu_id, StringTypes):
                from_otu_id = ObjectId(from_otu_id)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        self.bind_object.logger.info("开始导入sg_hc_heatmap_detail表")
        insert_data = list()
        with open(file_path, 'rb') as r:
            head = r.next().strip('\r\n')   #windows换行符
            head = re.split('\t', head)
            new_head = head[1:]
            for line in r:
                line = line.rstrip("\r\n")
                line = re.split('\t', line)
                sample_num = line[1:]
                classify_list = re.split(r"\s*;\s*", line[0])
                otu_detail = dict()
                otu_detail['hc_id'] = ObjectId(new_otu_id)
                for cf in classify_list:
                    if cf != "":
                        otu_detail[cf[0:3].lower()] = cf
                for i in range(0, len(sample_num)):
                    otu_detail[new_head[i]] = sample_num[i]
                # otu_detail['task_id'] = self.task_id
                insert_data.append(otu_detail)
        try:
            collection = self.db['sg_hc_heatmap_detail']
            collection.insert_many(insert_data)
            main_collection = self.db["sg_hc_heatmap"]
            self.bind_object.logger.info("开始刷新主表写树")
            main_collection.update({"_id": ObjectId(new_otu_id)},
                                    {"$set": {
                                        "sample_tree": sample_tree if sample_tree else "()",
                                        "species_tree": species_tree if species_tree else "()",
                                        "sample_list": sample_list if sample_list else [],
                                        "species_list": species_list if species_list else []}})
        except Exception as e:
            self.bind_object.logger.error("导入sg_hc_heatmap_detail表格信息出错:{}".format(e))
        else:
            self.bind_object.logger.info("导入sg_hc_heatmap_detail表格成功")
