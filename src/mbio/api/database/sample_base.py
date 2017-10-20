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
        :param file_path: 输入文件路径
        :return:
        """
        collection = self.db["sg_test_specimen"]
        results = {}
        if self.bind_object.option("in_fastq"):
            self.bind_object.logger.info("已经开始导表了！！！！！")
            try:
                bk = xlrd.open_workbook(info_file)
                sh = bk.sheet_by_name(u'Sheet1')
            except:
                self.bind_object.logger.info('样本信息表-表单名称不对')
                raise Exception('样本信息表-表单名称不对')
            nrows = sh.nrows
            for i in range(0, nrows):
                row_data = sh.row_values(i)
                if i == 0:
                    try:
                        sample_name_index = row_data.index(u'\u6837\u672c')  # 样本
                        platform_index = row_data.index(u'\u6d4b\u5e8f\u5e73\u53f0')  # 测序平台
                        strategy_index = row_data.index(u'\u6d4b\u5e8f\u7b56\u7565')  # 测序策略
                        primer_index = row_data.index(u'\u5f15\u7269')  # 引物
                        contract_number_index = row_data.index(u'\u5408\u540c\u53f7')  # 合同号
                        contract_sequence_number_index = row_data.index(u'\u7b7e\u8ba2\u6d4b\u5e8f\u91cf')  # 签订测序量
                        mj_number_index = row_data.index(u'\u7f8e\u5409\u7f16\u53f7')  # 美吉编号
                        client_name_index = row_data.index(u'\u7528\u6237\u540d')  # 用户名
                    except:
                        self.bind_object.logger.info("样本信息表的表头信息不全或不对应")
                        raise Exception('样本信息表表的表头信息不全或不对应')
                else:
                    if row_data[sample_name_index] == sample:
                        results["platform"] = row_data[platform_index]
                        results["strategy"] = row_data[strategy_index]
                        results["primer"] = row_data[primer_index]
                        results["contract_number"] = row_data[contract_number_index]
                        results["contract_sequence_number"] = row_data[contract_sequence_number_index]
                        results["mj_number"] = row_data[mj_number_index]
                        results["client_name"] = row_data[client_name_index]
                        results["sample_path"] = dir_path + '/' + sample + '.fq'
                        results["pipeline_type"] = "meta"
                    with open(info_txt, "r") as fr1:
                        for line in fr1:
                            tmp = line.strip().split("\t")
                            if not tmp[1] == sample:
                                continue
                            results["specimen_name"] = tmp[1]
                            results["sequence_num"] = tmp[3]
                            results["base_num"] = tmp[4]
                            results["mean_length"] = tmp[5]
                            results["min_length"] = tmp[6]
                            results["max_length"] = tmp[7]
                            results["file_path"] = [file_path + tmp[0].strip().split("\\")[-1]]
            sample_id = collection.insert_one(results).inserted_id
        else:
            self.bind_object.logger.info("居然没有导表！！！！！")
            with open(info_txt, "r") as fr1, open(info_file, "r")as fr2:
                lines1 = fr1.readlines()
                info_dic = defaultdict(list)
                for line in lines1[1:]:
                    line_split = line.strip().split("\t")
                    if line_split[1] == sample:
                        info_dic[line_split[1]] = line_split[3:]
                    else:
                        pass
                lines2 = fr2.readlines()
                for line in lines2[1:]:
                    tmp = line.strip().split("\t")
                    id_list = tmp[-2].split(",")
                    if tmp[1] == sample:
                        if id_list == 1:                   # 如果没有合并样本，则无需导样本表
                            sample_id = id_list[0]
                        else:
                            results["platform"] = tmp[2]
                            results["strategy"] = tmp[3]
                            results["primer"] = tmp[4]
                            results["contract_number"] = tmp[5].split(",")
                            results["contract_sequence_number"] = tmp[6].split(",")
                            results["mj_number"] = tmp[7].split(",")
                            results["client_name"] = tmp[8].split(",")
                            results["sample_path"] = dir_path + '/' + tmp[1] + '.fq'
                            results["specimen_name"] = tmp[1]
                            results["sequence_num"] = info_dic[tmp[1]][0]
                            results["base_num"] = info_dic[tmp[1]][1]
                            results["mean_length"] = info_dic[tmp[1]][2]
                            results["min_length"] = info_dic[tmp[1]][3]
                            results["max_length"] = info_dic[tmp[1]][4]
                            results["file_path"] = id_list
                            sample_id = collection.insert_one(results).inserted_id
                    else:
                        pass
        self.bind_object.logger.info("表格导入成功")
        return sample_id

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
    def add_sg_test_batch_specimen(self, table_id, sample_id, sample):
        collection = self.db["sg_test_batch_specimen"]
        results_list = []
        results = {}
        results["batch_id"] = ObjectId(table_id)
        results["specimen_id"] = ObjectId(sample_id)
        results["alias_name"] = sample
        results["dec"] = ''
        results_list.append(results)
        try:
            collection.insert_many(results_list)
            self.bind_object.logger.info("样本表格导入成功")
        except:
            self.bind_object.logger.info("样本表格导入出错")
            raise Exception('样本表格导入出错')

    @report_check
    def update_sg_test_batch_meta(self, table_id, info_file):
        contract_number_list = []  # 合同号
        client_name_list = []  # 用户名
        contract_sequence_number_list = []
        mj_number_list = []
        sample_list = []  # 所有样本
        if self.bind_object.option("in_fastq").is_set:
            try:
                bk = xlrd.open_workbook(info_file)
                sh = bk.sheet_by_name(u'Sheet1')
            except:
                self.bind_object.logger.info('样本信息表-表单名称不对')
                raise Exception('样本信息表-表单名称不对')
            nrows = sh.nrows
            for i in range(0, nrows):
                row_data = sh.row_values(i)
                if i == 0:
                    try:
                        sample_name_index = row_data.index(u'\u6837\u672c')  # 样本
                        platform_index = row_data.index(u'\u6d4b\u5e8f\u5e73\u53f0')  # 测序平台
                        strategy_index = row_data.index(u'\u6d4b\u5e8f\u7b56\u7565')  # 测序策略
                        primer_index = row_data.index(u'\u5f15\u7269')  # 引物
                        contract_number_index = row_data.index(u'\u5408\u540c\u53f7')  # 合同号
                        contract_sequence_number_index = row_data.index(u'\u7b7e\u8ba2\u6d4b\u5e8f\u91cf')  # 签订测序量
                        mj_number_index = row_data.index(u'\u7f8e\u5409\u7f16\u53f7')  # 美吉编号
                        client_name_index = row_data.index(u'\u7528\u6237\u540d')  # 用户名
                    except:
                        self.bind_object.logger.info("样本信息表的表头信息不全或不对应")
                        raise Exception('样本信息表表的表头信息不全或不对应')
                else:
                    platform = row_data[platform_index]
                    strategy = row_data[strategy_index]
                    primer = row_data[primer_index]
                    if row_data[sample_name_index] in sample_list:
                        pass
                    else:
                        sample_list.append(row_data[sample_name_index])
                    if row_data[contract_number_index] in contract_number_list:
                        pass
                    else:
                        contract_number_list.append(row_data[contract_number_index])
                    if row_data[client_name_index] in client_name_list:
                        pass
                    else:
                        client_name_list.append(row_data[client_name_index])
                    if row_data[contract_sequence_number_index] in contract_sequence_number_list:
                        pass
                    else:
                        contract_sequence_number_list.append(row_data[contract_sequence_number_index])
                    if row_data[mj_number_index] in mj_number_list:
                        pass
                    else:
                        mj_number_list.append(row_data[mj_number_index])
        else:
            with open(info_file)as fr:
                lines = fr.readlines()
                for line in lines[1:]:
                    tmp = line.strip().split("\t")
                    sample_list.append(tmp[0])
                    platform = tmp[1]
                    strategy = tmp[2]
                    if tmp[4] not in contract_number_list:
                        contract_number_list.append(tmp[4])
                    if tmp[5] not in contract_sequence_number_list:
                        contract_sequence_number_list.append(tmp[5])
                    if tmp[6] not in mj_number_list:
                        mj_number_list.append(tmp[6])
                    if tmp[7] not in client_name_list:
                        client_name_list.append(tmp[7])
                primer = lines[1][3]
        sample_num = len(sample_list)  # 样本个数
        contract = "_".join(contract_number_list)
        client_name = "_".join(client_name_list)
        batch_name = contract + '-' + client_name + '-' + primer + '-' + str(sample_num) + 'Samples-' + datetime.datetime.now().strftime(
            "%Y%m%d_%H%M%S")  # 多样性中样本集命名：合同号－用户名－引物－样本个数-时间戳
        collection = self.db['sg_test_batch']
        try:
            collection.update({'_id': ObjectId(table_id)}, {
                '$set': {'batch_name': batch_name, 'platform': platform, "strategy": strategy, "primer": primer,
                         "contract_number": contract_number_list, "client_name": client_name_list,
                         "contract_sequence_number": contract_sequence_number_list, "mj_number": mj_number_list}})
            self.bind_object.logger.info("主表更新成功")
        except:
            self.bind_object.logger.info("主表更新出错")
            raise Exception('主表更新出错')