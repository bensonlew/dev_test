# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.api.database.base import Base, report_check
import re
import datetime
import json
from bson.objectid import ObjectId
from types import StringTypes
# from biocluster.config import Config


class SubSample(Base):
    def __init__(self, bind_object):
        super(SubSample, self).__init__(bind_object)
        self._project_type = 'meta'
        # self._db_name = Config().MONGODB
        self.name_id = dict()  # otu表中样本名和id对照的字典
        self.otu_rep = dict()  # o

    def add_sg_otu(self, params, my_size, from_otu_table=0, name=None):
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
        task_id = result['task_id']
        if not name:
            name = "otu_subsample" + str(my_size) + '_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        insert_data = {
            "project_sn": project_sn,
            'task_id': task_id,
            'from_id': str(from_otu_table),
            'name': self.bind_object.sheet.main_table_name if self.bind_object.sheet.main_table_name else name,
            "params": params,
            'status': 'end',
            "level_id": json.dumps([9]),
            'desc': 'otu table after Otu Subsampe',
            "type": "otu_statistic",
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_otu"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

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

    def _prepare_otu_rep(self, from_otu_id):
        """
        实际运行的时候发现对每一行(即每一个otu)都去数据库里查询一次，并获取otu_rep的时候，效率非常低，需要很长的时间， 因此，需要对mongo做一次查询， 将属于一个otu_id的otu_rep全部去读出来， 放到内存当中， 以提高效率
        """
        self.bind_object.logger.info("开始依据otu_id: {}查询所有的代表序列".format(from_otu_id))
        collection = self.db["sg_otu_detail"]
        results = collection.find({"otu_id": from_otu_id})
        for result in results:
            self.otu_rep[result['otu']] = result["otu_rep"]
        self.bind_object.logger.info("代表序列查询完毕")

    @report_check
    def add_sg_otu_detail(self, file_path, from_otu_id, new_otu_id, new_samples=False):
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
        self.bind_object.logger.info("开始导入sg_otu_detail表")
        self._prepare_otu_rep(from_otu_id)
        insert_data = list()
        with open(file_path, 'rb') as r:
            head = r.next().strip('\r\n')
            head = re.split('\t', head)
            if head[-1] == "taxonomy":
                new_head = head[1:-1]
            else:
                new_head = head[1:]
            for line in r:
                line = line.rstrip("\r\n")
                line = re.split('\t', line)
                otu_detail = dict()
                # OTU表可能有taxonomy列， 也可能没有， 需要适应
                if len(re.split("; ", line[0])) > 1:
                    sample_num = line[1:]
                    otu = re.split("; ", line[0])[-1]
                    classify_list = re.split(r"\s*;\s*", line[0])
                else:
                    sample_num = line[1:-1]
                    otu = line[0]
                    otu_detail['otu'] = line[0]
                    classify_list = re.split(r"\s*;\s*", line[-1])

                otu_detail['otu_id'] = new_otu_id
                for cf in classify_list:
                    if cf != "":
                        otu_detail[cf[0:3].lower()] = cf
                for i in range(0, len(sample_num)):
                    otu_detail[new_head[i]] = int(sample_num[i])

                if otu not in self.otu_rep:
                    raise Exception("意外错误，otu_id: {}和otu: {}在sg_otu_detail表里未找到".format(from_otu_id, line[0]))
                otu_detail['otu_rep'] = self.otu_rep[otu]
                otu_detail['task_id'] = self.task_id
                insert_data.append(otu_detail)
        try:
            collection = self.db['sg_otu_detail']
            collection.insert_many(insert_data)
        except Exception as e:
            self.bind_object.logger.error("导入sg_otu_detail表格信息出错:{}".format(e))
        else:
            self.bind_object.logger.info("导入sg_otu_detail表格成功")
        # 导入sg_otu_specimen表
        self.bind_object.logger.info("开始导入sg_otu_specimen表")
        if not new_samples:
            insert_data = list()
            for sp in new_head:
                my_data = dict()
                my_data['otu_id'] = new_otu_id
                my_data["specimen_id"] = self.name_id[sp]
                insert_data.append(my_data)
            collection = self.db['sg_otu_specimen']
            collection.insert_many(insert_data)

    @report_check
    def add_sg_otu_detail_level(self, otu_path, from_otu_table, level):
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
        task_id = result['task_id']
        covered_level = list()
        if "level_id" in result:
            covered_level = json.loads(result["level_id"])
            covered_level.append(int(level))
        else:
            covered_level.append(int(level))
        covered_level = list(set(covered_level))
        covered_level.sort()
        result["level_id"] = json.dumps(covered_level)
        collection.update({"_id": from_otu_table}, {"$set": result}, upsert=False)
        insert_data = list()
        with open(otu_path, 'rb') as r:
            head = r.next().strip('\r\n')
            head = re.split('\t', head)
            if head[-1] == "taxonomy":
                new_head = head[1:-1]
            else:
                new_head = head[1:]
            for line in r:
                line = line.rstrip("\r\n")
                line = re.split('\t', line)
                otu_detail = dict()

                if len(re.split("; ", line[0])) > 1:
                    sample_num = line[1:]
                    classify_list = re.split(r"\s*;\s*", line[0])
                else:
                    sample_num = line[1:-1]
                    otu_detail['otu'] = line[0]
                    classify_list = re.split(r"\s*;\s*", line[-1])

                otu_detail['otu_id'] = from_otu_table
                otu_detail['project_sn'] = project_sn
                otu_detail['task_id'] = task_id
                otu_detail["level_id"] = int(level)
                for cf in classify_list:
                    if cf != "":
                        otu_detail[cf[0:3].lower()] = cf
                count = 0
                for i in range(0, len(sample_num)):
                    otu_detail[new_head[i]] = int(sample_num[i])
                    count += int(sample_num[i])
                otu_detail["total_"] = count
                insert_data.append(otu_detail)
        try:
            collection = self.db['sg_otu_detail_level']
            collection.insert_many(insert_data)
        except Exception as e:
            self.bind_object.logger.info("导入sg_otu_detail_level表格失败：{}".format(e))
            raise Exception("导入sg_otu_detail_level表格失败：{}".format(e))
        else:
            self.bind_object.logger.info("导入sg_otu_detail_copy表格成功")
