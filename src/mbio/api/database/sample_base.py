# -*- coding: utf-8 -*-
# __author__ = 'shijin'

import os
import json
from bson.objectid import ObjectId
from types import StringTypes
from biocluster.config import Config
from biocluster.api.database.base import Base, report_check
import datetime
import xlrd
from collections import defaultdict


class SampleBase(Base):
    def __init__(self, bind_object):
        super(SampleBase, self).__init__(bind_object)
        self._db_name = "samplebase"

    @report_check
    def add_sg_test_specimen_meta(self, sample, info_txt, info_file, dir_path, file_path=None):
        """
        :param sample:样本名
        :param info_txt: 样本统计信息
        :param info_file: 样本基本信息
        :param dir_path: 样本集磁盘路径
        :param file_path: list,序列和磁盘的对应关系
        :return:
        """
        collection = self.db["sg_test_specimen"]
        results = {}
        self.bind_object.logger.info("样本信息导表！")
        with open(info_txt, "r") as fr1, open(info_file, "r")as fr2:
            lines1 = fr1.readlines()
            info_dic = defaultdict(list)
            for line in lines1[1:]:
                line_split = line.strip().split("\t")
                if line_split[1] == sample:
                    info_dic[line_split[1]] = line_split
                else:
                    pass
            lines2 = fr2.readlines()
            for line in lines2[1:]:
                tmp = line.strip().split("\t")
                if tmp[0] == sample:
                    results["pipeline_type"] = "meta"
                    results["platform"] = tmp[1]
                    results["strategy"] = tmp[2]
                    results["primer"] = tmp[3]
                    results["contract_number"] = tmp[4].split(",")
                    results["contract_sequence_number"] = tmp[5].split(",")
                    results["mj_number"] = tmp[6].split(",")
                    results["client_name"] = tmp[7].split(",")
                    results["sample_path"] = dir_path + '/' + tmp[0] + '.fq'
                    results["specimen_name"] = tmp[0]
                    results["sequence_num"] = info_dic[tmp[0]][3]
                    results["base_num"] = info_dic[tmp[0]][4]
                    results["mean_length"] = info_dic[tmp[0]][5]
                    results["min_length"] = info_dic[tmp[0]][6]
                    results["max_length"] = info_dic[tmp[0]][7]
                    if self.bind_object.option("in_fastq"):
                        file_path_list = []
                        file_alias_list = []
                        file_list = info_dic[tmp[0]][0].strip().split(",")  # 样本来源于哪些文件
                        for j in file_list:
                            files = j.strip().split("/")[-1]
                            for i in range(len(file_path)):
                                self.bind_object.logger.info(file_path)
                                file_name = file_path[i]["file_path"]
                                if self.bind_object.option("in_fastq").format == 'sequence.fastq':  # 序列时文件中的名称为文件磁盘原名
                                    if files == file_name.strip().split("/")[-1]:
                                        file_path_list.append(file_name)
                                        file_alias_list.append(file_path[i]["file_alias"])
                                else:  # 输入为文件夹时，文件中的名称已是别名
                                    if files == file_path[i]["file_alias"]:
                                        file_path_list.append(file_name)
                                        file_alias_list.append(file_path[i]["file_alias"])
                    else:
                        file_path_list = tmp[-2].split(",")
                    results["file_path"] = file_path_list
                    sample_id = collection.insert_one(results).inserted_id
                else:
                    pass
        self.bind_object.logger.info("表格导入成功")
        return sample_id, file_alias_list

    @report_check
    def add_sg_test_specimen_rna(self, sample, stat_path, file_sample):
        collection = self.db["sg_test_specimen"]
        results = {}
        with open(stat_path, "r") as files:
            for line in files:
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
        results["file_name"] = []
        for key in file_sample.keys():
            if file_sample[key] == sample:
                results["file_name"].append(key)
        results["file_name"] = ",".join(results["file_name"])  # 考虑到双端序列，进行这样的处理
        sample_id = collection.insert_one(results).insert_id
        return sample_id

    @report_check
    def add_sg_test_batch_specimen(self, table_id, sample_id, sample, file_alias_list):
        collection = self.db["sg_test_batch_specimen"]
        results_list = []
        results = {}
        results["batch_id"] = ObjectId(table_id)
        results["specimen_id"] = ObjectId(sample_id)
        results["alias_name"] = sample
        results["dec"] = ''
        results["file_alias_name"] = file_alias_list
        results_list.append(results)
        try:
            collection.insert_many(results_list)
            self.bind_object.logger.info("样本表格导入成功")
        except:
            self.bind_object.logger.info("样本表格导入出错")
            raise Exception('样本表格导入出错')

    @report_check
    def update_sg_test_batch_meta(self, table_id, info_file):
        platform_list = []  # 平台
        strategy_list = []  # 测序策略
        primer_list = []  # 引物
        contract_number_list = []  # 合同号
        client_name_list = []  # 用户名
        contract_sequence_number_list = []
        mj_number_list = []
        sample_list = []  # 所有样本
        with open(info_file)as fr:
            lines = fr.readlines()
            for line in lines[1:]:
                tmp = line.strip().split("\t")
                sample_list.append(tmp[0])
                for platform in tmp[1].split(","):
                    if platform not in platform_list:
                        platform_list.append(platform)
                for strategy in tmp[2].split(","):
                    if strategy not in strategy_list:
                        strategy_list.append(strategy)
                for primer in tmp[3].split(","):
                    if primer not in primer_list:
                        primer_list.append(primer)
                for contract_number in tmp[4].split(","):
                    if contract_number not in contract_number_list:
                        contract_number_list.append(contract_number)
                for contract_sequence_number in tmp[5].split(","):
                    if contract_sequence_number not in contract_sequence_number_list:
                        contract_sequence_number_list.append(contract_sequence_number)
                for mj_number in tmp[6].split(","):
                    if mj_number not in mj_number_list:
                        mj_number_list.append(mj_number)
                for client_name in tmp[7].split(","):
                    if client_name not in client_name_list:
                        client_name_list.append(client_name)

        sample_num = len(sample_list)  # 样本个数
        contract = "_".join(contract_number_list)
        client_name = "_".join(client_name_list)
        primer = "_".join(primer_list)
        batch_name = contract + '-' + client_name + '-' + primer + '-' + str(sample_num) + 'Samples-' + \
                     datetime.datetime.now().strftime("%Y%m%d_%H%M%S")  # 多样性中样本集命名：合同号－用户名－引物－样本个数-时间戳
        collection = self.db['sg_test_batch']
        try:
            collection.update({'_id': ObjectId(table_id)}, {
                '$set': {'batch_name': batch_name, 'platform': platform_list, "strategy": strategy_list, "primer": primer_list,
                         "sample_num": sample_num, "contract_number": contract_number_list, "client_name": client_name_list,
                         "contract_sequence_number": contract_sequence_number_list, "mj_number": mj_number_list}})
            self.bind_object.logger.info("主表更新成功")
        except:
            self.bind_object.logger.info("主表更新出错")
            raise Exception('主表更新出错')