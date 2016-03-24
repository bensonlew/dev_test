# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes


class SubSample(Base):
    def __init__(self, bind_object):
        super(SubSample, self).__init__(bind_object)
        self._db_name = "sanger"
        self.name_id = dict()  # otu表中样本名和id对照的字典

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

    def _get_task_info(self, otu_id):
        collection = self.db['sg_otu']
        result = collection.find_one({'_id': otu_id})
        if not result:
            raise Exception("无法根据传入的_id:{}在sg_otu表里找到相应的记录".format(otu_id))
        self.project_sn = result['project_sn']
        self.task_id = result['task_id']

    @report_check
    def add_sg_otu_detail(self, file_path, from_otu_id, new_otu_id):
        if not isinstance(from_otu_id, ObjectId):
            if isinstance(from_otu_id, StringTypes):
                from_otu_id = ObjectId(from_otu_id)
            else:
                raise Exception("from_otu_id必须为ObjectId对象或其对应的字符串!")
        if not isinstance(new_otu_id, ObjectId):
            if isinstance(new_otu_id, StringTypes):
                new_otu_id = ObjectId(new_otu_id)
            else:
                raise Exception("new_otu_id必须为ObjectId对象或其对应的字符串!")
        self._get_name_id(from_otu_id)
        self._get_task_info(new_otu_id)
        # 导入sg_otu_detail表
        insert_data = list()
        with open(file_path, 'rb') as r:
            head = r.next().strip('\r\n')
            head = re.split('\t', head)
            new_head = head[1:-1]
            for line in r:
                line = line.rstrip("\r\n")
                query_dict = dict()  # 构建查询字典，用于获取otu_rep(代表序列)
                line = re.split('\t', line)
                query_dict["otu"] = line[0]
                query_dict["otu_id"] = from_otu_id
                sample_num = line[1:-1]
                classify_list = re.split(r"\s*;\s*", line[-1])
                otu_detail = dict()
                otu_detail['otu'] = line[0]
                otu_detail['otu_id'] = new_otu_id
                for cf in classify_list:
                    if cf != "":
                        otu_detail[cf[0:3]] = cf
                for i in range(0, len(sample_num)):
                    otu_detail[new_head[i]] = sample_num[i]
                collection = self.db['sg_otu_detail']
                result = collection.find_one(query_dict)
                if not result:
                    raise Exception("未知错误，出入的otu_id: {}和otu: {}在sg_otu_detail表中未找到".format(from_otu_id, line[0]))
                otu_detail['otu_rep'] = result["otu_rep"]
                otu_detail['task_id'] = self.task_id
                insert_data.append(otu_detail)
        self.bind_object.logger.debug(insert_data)
        try:
            collection = self.db['sg_otu_detail']
            collection.insert_many(insert_data)
        except Exception as e:
            self.bind_object.logger.error("导入sg_otu_detail表格信息出错:{}".format(e))
        else:
            self.bind_object.logger.info("导入sg_otu_detail表格成功")
        # 导入sg_otu_specimen表
        insert_data = list()
        for sp in new_head:
            my_data = dict()
            my_data['otu_id'] = new_otu_id
            my_data["specimen_id"] = self.name_id[sp]
            insert_data.append(my_data)
        collection = self.db['sg_otu_specimen']
        collection.insert_many(insert_data)
