# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.api.database.base import Base, report_check
import re
import datetime
from bson.objectid import ObjectId
from types import StringTypes
from biocluster.config import Config


class ClusterAnalysis(Base):
    """
    用于聚类分析导入OTU表， 由于level的关系， 这个OTU表没有代表序列， OTU的分类信息可能不完整
    样本与OTU的对应信息不可在sg_otu_speciem表里找到，因此， 这张OTU也不可以在之后的分析中用到
    """
    def __init__(self, bind_object):
        super(ClusterAnalysis, self).__init__(bind_object)
        self._db_name = Config().MONGODB
        self.task_id = ""
        self.name_id = dict()

    @report_check
    def add_sg_otu(self, params, from_otu_table, name=None, newick_id=None):
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
            name = "community_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        insert_data = {
            "project_sn": project_sn,
            'task_id': self.task_id,
            'from_id': str(from_otu_table),
            'name': self.bind_object.sheet.main_table_name if self.bind_object.sheet.main_table_name else name,
            "params": params,
            "newick_id": newick_id,
            'status': 'end',
            'desc': 'otu table after Cluster Analysis',
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "show": 0,
            "type": "otu_group_analyse"
        }
        collection = self.db["sg_otu"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    @report_check
    def add_sg_otu_detail(self, file_path, new_otu_id, from_otu_id):
        if from_otu_id != 0 and not isinstance(from_otu_id, ObjectId):
            if isinstance(from_otu_id, StringTypes):
                from_otu_id = ObjectId(from_otu_id)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        self.bind_object.logger.info("开始导入sg_otu_detail表")
        self._get_name_id(from_otu_id)
        insert_data = list()
        with open(file_path, 'rb') as r:
            head = r.next().strip('\r\n')
            head = re.split('\t', head)
            new_head = head[1:]
            for line in r:
                line = line.rstrip("\r\n")
                line = re.split('\t', line)
                sample_num = line[1:]
                classify_list = re.split(r"\s*;\s*", line[0])
                otu_detail = dict()
                otu_detail['otu_id'] = ObjectId(new_otu_id)
                for cf in classify_list:
                    if cf != "":
                        otu_detail[cf[0:3].lower()] = cf
                for i in range(0, len(sample_num)):
                    otu_detail[new_head[i]] = sample_num[i]
                otu_detail['task_id'] = self.task_id
                insert_data.append(otu_detail)
        try:
            collection = self.db['sg_otu_detail']
            collection.insert_many(insert_data)
        except Exception as e:
            self.bind_object.logger.error("导入sg_otu_detail表格信息出错:{}".format(e))
        else:
            self.bind_object.logger.info("导入sg_otu_detail表格成功")
        self.bind_object.logger.info("开始导入sg_otu_specimen表")
        insert_data = list()
        for sp in new_head:
            my_data = dict()
            my_data['otu_id'] = ObjectId(new_otu_id)
            my_data["specimen_id"] = self.name_id[sp]
            insert_data.append(my_data)
        collection = self.db['sg_otu_specimen']
        collection.insert_many(insert_data)

    def _get_name_id(self, from_otu_id):
        collection = self.db['sg_otu_specimen']
        results = collection.find({"otu_id": from_otu_id})
        if not results.count():
            raise Exception("otu_id:{}未在otu_sg_specimen表里找到相应的记录".format(from_otu_id))
        sp_ids = list()
        for result in results:
            sp_ids.append(result['specimen_id'])
        collection = self.db['sg_specimen']
        for id_ in sp_ids:
            result = collection.find_one({"_id": id_})
            if not result:
                raise Exception("意外错误， id: {}在sg_otu_specimen表中找到，但未在sg_specimen表中出现")
            self.name_id[result["specimen_name"]] = id_
