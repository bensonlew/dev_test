#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__: konghualei 20170424

from pymongo import MongoClient
from bson.objectid import ObjectId
import types
from types import StringTypes
import re
import json, time
import pandas as pd
import numpy as np
import datetime, os
from bson.son import SON
from collections import Counter
import glob
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
import math


class RefrnaExpress(Base):
    def __init__(self, bind_object):
        super(RefrnaExpress, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'

    def add_express(self,
                    rsem_dir=None,
                    group_fpkm_path=None,
                    transcript_fasta_path=None,
                    is_duplicate=None,
                    class_code=None,
                    samples=None,
                    params=None,
                    name=None,
                    express_diff_id=None,
                    bam_path=None,
                    major=True,
                    distri_path=None):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        time_now = datetime.datetime.now()
        table_name = name if name else 'ExpressStat_' + time_now.strftime("%Y%m%d_%H%M%S")
        if isinstance(params, dict):
            params = json.dumps(params, sort_keys=True, separators=(',', ':'))
        insert_data = dict(project_sn=project_sn,
                           task_id=task_id,
                           name=table_name,
                           desc='表达量计算主表',
                           created_ts=time_now.strftime('%Y-%m-%d %H:%M:%S'),
                           params=params,
                           specimen=samples,
                           status='end',
                           bam_path=bam_path,
                           transcript_fasta_path=transcript_fasta_path,
                           is_duplicate=is_duplicate)
        if params:
            insert_data["genes"] = True
            if params["express_method"] == "rsem":
                insert_data["trans"] = True
            if params["express_method"].lower() == "featurecounts":
                insert_data["trans"] = False
            if params['group_detail']:
                if is_duplicate:
                    insert_data["group"] = params["group_detail"].keys()
        if express_diff_id:
            insert_data["express_diff_id"] = express_diff_id
        collection = db['sg_express']
        express_id = collection.insert_one(insert_data).inserted_id
        print "插入主表id是{}".format(express_id)
        value_type = params["type"]
        sample_group = "sample"
        method = params["express_method"]
        if major:
            rsem_files = os.listdir(rsem_dir)
            # sample_group = "sample"
            for f in rsem_files:
                if re.search(r'^genes\.TMM', f):
                    fpkm_path = rsem_dir + "/" + f
                    count_path = rsem_dir + '/genes.counts.matrix'
                    # query_type=None,value_type=None, method=None, sample_group=None

                    self.add_express_detail(express_id, count_path, fpkm_path, class_code, 'gene',
                                            value_type, method, sample_group)
                    self.add_express_gragh(express_id,
                                           distribution_path_log2=distri_path + "/log2gene_distribution.xls", \
                                           distribution_path_log10=distri_path + "/log10gene_distribution.xls", \
                                           distribution_path=distri_path + "/gene_distribution.xls",
                                           sample_group="sample", query_type="gene")
                    self.add_express_box(express_id, fpkm_path=rsem_dir + "/" + f,
                                         sample_group="sample", query_type="gene")
                    if is_duplicate:
                        if value_type == 'fpkm':
                            gene_group_fpkm_path = group_fpkm_path + "/Group.genes_genes.TMM.fpkm.matrix"
                        if value_type == 'tpm':
                            gene_group_fpkm_path = group_fpkm_path + "/Group.genes_genes.TMM.EXPR.matrix"
                        if os.path.exists(gene_group_fpkm_path):
                            self.add_express_group_detail(express_id, gene_group_fpkm_path, "gene",
                                                          value_type, "rsem", "group")
                        self.add_express_gragh(express_id,
                                               distribution_path_log2=distri_path + "/log2GroupGenes_distribution.xls", \
                                               distribution_path_log10=distri_path + "/log10GroupGenes_distribution.xls", \
                                               distribution_path=distri_path + "/GroupGenes_distribution.xls",
                                               sample_group="group", query_type="gene")
                        self.add_express_box(express_id, fpkm_path=gene_group_fpkm_path,
                                             sample_group="group", query_type="gene")
                elif re.search(r'^transcripts\.TMM', f):
                    fpkm_path = rsem_dir + "/" + f
                    count_path = rsem_dir + '/transcripts.counts.matrix'
                    self.add_express_detail(express_id, count_path, fpkm_path, class_code,
                                            'transcript', value_type, method, sample_group)
                    self.add_express_gragh(express_id,
                                           distribution_path_log2=distri_path + "/log2transcript_distribution.xls", \
                                           distribution_path_log10=distri_path + "/log10transcript_distribution.xls", \
                                           distribution_path=distri_path + "/transcript_distribution.xls",
                                           sample_group="sample", query_type="transcript")
                    self.add_express_box(express_id, fpkm_path=rsem_dir + "/" + f,
                                         sample_group="sample", query_type="transcript")
                    if is_duplicate:
                        if value_type == 'fpkm':
                            trans_group_fpkm_path = group_fpkm_path + "/Group.trans_transcripts.TMM.fpkm.matrix"
                        if value_type == 'tpm':
                            trans_group_fpkm_path = group_fpkm_path + "/Group.trans_transcripts.TMM.EXPR.matrix"
                        if os.path.exists(trans_group_fpkm_path):
                            self.add_express_group_detail(express_id, trans_group_fpkm_path,
                                                          "transcript", value_type, "rsem", "group")
                        self.add_express_gragh(express_id,
                                               distribution_path_log2=distri_path + "/log2GroupTrans_distribution.xls", \
                                               distribution_path_log10=distri_path + "/log10GroupTrans_distribution.xls", \
                                               distribution_path=distri_path + "/GroupTrans_distribution.xls",
                                               sample_group="group", query_type="transcript")
                        self.add_express_box(express_id, fpkm_path=trans_group_fpkm_path,
                                             sample_group="group", query_type="transcript")
                elif re.search(r'\.genes\.results$', f):
                    sample = f.split('.genes.results')[0]
                    file_ = rsem_dir + "/" + f
                    self.add_express_specimen_detail(express_id, file_, 'gene', sample)
                elif re.search(r'\.isoforms\.results$', f):
                    sample = f.split('.isoforms.results')[0]
                    file_ = rsem_dir + "/" + f
                    self.add_express_specimen_detail(express_id, file_, 'transcript', sample)
        return express_id

    def add_express_feature(self, feature_dir=None, group_fpkm_path=None, is_duplicate=None,
                            class_code=None, samples=None, params=None, name=None,
                            express_diff_id=None, bam_path=None, major=True, distri_path=None):
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        insert_data = {
            'project_sn'  : project_sn,
            'task_id'     : task_id,
            'name'        : name if name else 'ExpressStat_' + str(
                datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'desc'        : '表达量计算主表',
            'created_ts'  : datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params'      : (
            json.dumps(params, sort_keys=True, separators=(',', ':')) if isinstance(params,
                                                                                    dict) else params),
            'specimen'    : samples,
            'status'      : 'end',
            'bam_path'    : bam_path,
            'is_duplicate': is_duplicate
        }
        sample_group = "sample"
        if params:
            insert_data["genes"] = True
            insert_data["trans"] = False
            if params['group_detail']:
                if is_duplicate:
                    insert_data["group"] = params["group_detail"].keys()
        if express_diff_id:
            insert_data["express_diff_id"] = express_diff_id
        collection = db['sg_express']
        express_id = collection.insert_one(insert_data).inserted_id
        print "插入主表id是{}".format(express_id)
        value_type = params["type"]  # fpkm或者是tpm
        method = params["express_method"]
        if major:
            if value_type == 'fpkm':
                fpkm_path = feature_dir + "/fpkm_tpm.fpkm.xls"
            if value_type == 'tpm':
                fpkm_path = feature_dir + "/fpkm_tpm.tpm.xls"
            count_path = feature_dir + "/count.xls"
            self.add_express_detail(express_id, count_path, fpkm_path, class_code, 'gene',
                                    value_type, method, sample_group)
            self.add_express_gragh(express_id,
                                   distribution_path_log2=distri_path + "/{}/log2gene_distribution.xls".format(
                                       value_type), \
                                   distribution_path_log10=distri_path + "/{}/log10gene_distribution.xls".format(
                                       value_type), \
                                   distribution_path=distri_path + "/{}/gene_distribution.xls".format(
                                       value_type), sample_group="sample", query_type="gene")
            self.add_express_box(express_id, fpkm_path=fpkm_path, sample_group="sample",
                                 query_type="gene")
            if is_duplicate:
                if value_type == 'fpkm':
                    fpkm_group_path = group_fpkm_path + "/group.fpkm.xls"
                if value_type == 'tpm':
                    fpkm_group_path = group_fpkm_path + "/group.tpm.xls"
                self.add_express_group_detail(express_id, fpkm_group_path, "gene", value_type,
                                              "featurecounts", "group")

                self.add_express_gragh(express_id,
                                       distribution_path_log2=distri_path + "/group/{}/log2GroupGenes_distribution.xls".format(
                                           value_type), \
                                       distribution_path_log10=distri_path + "/group/{}/log10GroupGenes_distribution.xls".format(
                                           value_type), \
                                       distribution_path=distri_path + "/group/{}/GroupGenes_distribution.xls".format(
                                           value_type), sample_group="group", query_type="gene")
                self.add_express_box(express_id,
                                     fpkm_path=distri_path + "/group/group.{}.xls".format(
                                         value_type), sample_group="group", query_type="gene")
            for files in os.listdir(feature_dir):
                m_ = re.search(r'vs', files)
                if m_:
                    path = feature_dir + "/" + files
                    self.add_express_specimen_detail_feature(express_id, feature_dir + "/" + files)
                else:
                    self.bind_object.logger.error("没有找到正确的specimen路径！")
        return express_id

    def add_express_specimen_detail_feature(self, express_id, feature_result):
        """featurecounts单个样本的表达量信息"""
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
            else:
                raise Exception('express_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(feature_result):
            raise Exception('rsem_result所指定的路径：{}不存在，请检查！'.format(feature_result))
        with open(feature_result, 'r+') as f1:
            header = f1.readline().strip().split("\t")
            sample = header[6:]
            sample_num = len(sample)
            data_list = []
            for lines in f1:
                line = lines.strip().split("\t")
                insert_data = []
                for i in range(len(header)):
                    insert_data += [(header[i], line[i])]
                insert_data = SON(insert_data)
                data_list.append(insert_data)
        try:
            collection = db["sg_express_specimen_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入单样本表达量矩阵：%s信息出错:%s" % (feature_result, e))
        else:
            self.bind_object.logger.info("导入单样本表达量矩阵: %s信息成功!" % feature_result)

    def add_express_group_detail(self, express_id, group_fpkm_path=None, query_type=None,
                                 value_type=None, method=None, sample_group=None):
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        from math import log
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
            else:
                raise Exception('express_id必须为ObjectId对象或其对应的字符串！')
        with open(group_fpkm_path, 'r+') as f1:
            data_list = []
            group_name = f1.readline().strip().split("\t")
            group_num = len(group_name)
            if not query_type:
                raise Exception("请设置query_type参数！")
            if not method:
                raise Exception("请设置method参数！")
            if not value_type:
                raise Exception("请设置value_type参数！")
            if not sample_group:
                raise Exception("请设置sample_group参数！")
            for lines in f1:
                line = lines.strip().split("\t")
                seq_id = line[0]
                insert_data = [
                    ("type", query_type),
                    ("value_type", value_type),
                    ("method", method),
                    ("sample_group", sample_group),
                    ("express_id", express_id)
                ]
                fpkm_data = line[1:]
                for i in range(len(fpkm_data)):
                    data_log2 = log(float(fpkm_data[i]) + 1) / log(2)
                    data_log10 = log(float(fpkm_data[i]) + 1) / log(10)
                    insert_data += [
                        ('{}_log2_fpkm'.format(group_name[i]), float(data_log2)),
                        ('{}_log10_fpkm'.format(group_name[i]), float(data_log10))
                    ]
                insert_data = SON(insert_data)
                data_list.append(insert_data)
        try:
            collection = db["sg_express_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            bind_object.logger.error("导入表达量矩阵group信息出错:%s" % e)
            # print ("导入表达量矩阵信息出错:%s" % e)
        else:
            # print ("导入表达量矩阵信息成功!")
            bind_object.logger.info("导入表达量矩阵group信息成功!")

    # @report_check
    def add_express_detail(self, express_id, count_path, fpkm_path, class_code=None,
                           query_type=None, value_type=None, method=None, sample_group=None):
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        import math
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
            else:
                raise Exception('express_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(count_path):
            raise Exception('count_path:{}所指定的路径不存在，请检查！'.format(count_path))
        if not os.path.exists(fpkm_path):
            raise Exception('fpkm_path:{}所指定的路径不存在，请检查！'.format(fpkm_path))

        def class_code_get(class_code, query_type):
            if class_code:
                if os.path.exists(class_code):
                    with open(class_code, 'r+') as cc:
                        class_code_dict = {}
                        for lines in cc:
                            line = lines.strip().split("\t")
                            if query_type == "gene":
                                if line[1] not in class_code_dict.keys():
                                    class_code_dict[line[1]] = str(line[2])
                                else:
                                    pass
                            if query_type == "transcript":
                                if line[0] not in class_code_dict.keys():
                                    class_code_dict[line[0]] = str(line[2])
                                else:
                                    pass
                    return class_code_dict

        data_list = list()
        count_dict = {}
        sample_count = {}
        if class_code:
            class_code_info = class_code_get(class_code=class_code, query_type=query_type)
        with open(count_path, 'rb') as c, open(fpkm_path, 'rb') as f:
            samples = c.readline().strip().split("\t")
            for sam in samples:
                sample_count[sam] = 0
            c.readline()
            for line in c:
                line = line.strip().split('\t')
                count_dict[line[0]] = line[1:]
                count = line[1:]
                for i in range(len(count)):
                    if float(count[i]) > 0:
                        sample_count[samples[i]] += 1
            f.readline()
            for l in f:
                l = l.strip().split('\t')
                seq_id = l[0]
                if re.search(r'(,)', seq_id):
                    sequence_id = seq_id.split(",")[0]  # 以 ',' 为分隔符切割序列id和gene_name
                    gene_name = seq_id.split(",")[1]
                else:
                    sequence_id = seq_id
                    gene_name = None
                fpkm = l[1:]

                if class_code:
                    if class_code_info:
                        if sequence_id in class_code_info.keys():
                            _class_code = class_code_info[sequence_id]
                            if _class_code != "=":
                                _class = True
                            else:
                                _class = False
                        else:
                            _class = None
                else:
                    _class = None
                    self.bind_object.logger.error("更新class_code信息出错！:%s" % e)
                    # raise Exception("{}class_code信息没有找到，请确认:{}!".format(seq_id,class_code))

                data = [
                    ('seq_id', sequence_id),
                    ('type', query_type),
                    ('express_id', express_id),
                    ("value_type", value_type),
                    ("is_new", _class),
                    ("method", method),
                    # ("gene_name", gene_name), #添加gene_name信息
                    ("sample_group", sample_group)
                ]
                for i in range(len(samples)):
                    log2_fpkm = math.log(float(fpkm[i]) + 1) / math.log(2)
                    log10_fpkm = math.log(float(fpkm[i]) + 1) / math.log(10)
                    data += [
                        ('{}_count'.format(samples[i]), float(count_dict[seq_id][i])),
                        ('{}_fpkm'.format(samples[i]), float(fpkm[i])),
                        ('{}_sum'.format(samples[i]), sample_count[samples[i]]),
                        ('{}_log2_fpkm'.format(samples[i]), float(log2_fpkm)),
                        ('{}_log10_fpkm'.format(samples[i]), float(log10_fpkm)),
                    ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = db["sg_express_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            # bind_object.logger.error("导入表达量矩阵信息出错:%s" % e)
            print ("导入表达量矩阵信息出错:%s" % e)
        else:
            print ("导入表达量矩阵信息成功!")
            # bind_object.logger.info("导入表达量矩阵信息成功!")

    # @report_check
    def add_express_gragh(self, express_id, distribution_path_log2, distribution_path_log10,
                          distribution_path, sample_group, query_type=None):
        with open(distribution_path_log2, 'r+') as f1:
            samples = f1.readline().strip().split("\t")[1:]
        if not samples:
            raise Exception("没有获取到samples，请检查！")
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]

        def stat(fpkm_data, density, log=None):
            tmp = []
            if len(fpkm_data) != len(density):
                raise Exception("density必须和fpkm长度相等！")
            else:
                for i in range(len(fpkm_data)):
                    if not log:
                        tmp.append(
                            {"fpkm": round(fpkm_data[i], 6), "density": round(density[i], 6)})
                return tmp

        dflog2 = pd.read_table(distribution_path_log2)
        dflog10 = pd.read_table(distribution_path_log10)
        df = pd.read_table(distribution_path)
        samples = df.columns[1:]
        data_list = []
        for i in samples:
            insert_data = [
                ('express_id', express_id),
                ('type', query_type),
                ('specimen', i),
                ('sample_group', sample_group)
            ]
            tmp = stat(fpkm_data=df["fpkm"], density=df[i])
            tmplog2 = stat(fpkm_data=dflog2["log2fpkm"], density=dflog2[i])
            tmplog10 = stat(fpkm_data=dflog10["log10fpkm"], density=dflog10[i])
            insert_data.append(('data', tmp))
            insert_data.append(('data_log2', tmplog2))
            insert_data.append(('data_log10', tmplog10))
            insert_data = SON(insert_data)
            data_list.append(insert_data)
        try:
            collection = db["sg_express_gragh"]
            collection.insert_many(data_list)
        except Exception, e:
            print ("导入表达量矩阵作图数据：%s信息出错:%s" % (distribution_path_log2, e))
        else:
            print ("导入表达量矩阵作图数据: %s信息成功!" % distribution_path_log2)

    # @report_check
    def add_express_box(self, express_id, fpkm_path, sample_group, query_type=None):
        with open(fpkm_path, 'r+') as f1:
            samples = f1.readline().strip().split("\t")
        if not samples:
            raise Exception("没有获取到samples，请检查！")

        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]

        def log_value(value, log):
            """获取log值"""
            if log == 2:
                return np.log2(value.apply(lambda x: x + 1))
            elif log == 10:
                return np.log10(value.apply(lambda x: x + 1))
            else:
                return value

        def box_info(fpkm, samples, log=None):
            box = {}
            gene_list = {}
            for sam in samples:
                gene_list[sam] = {}
                box[sam] = {}
                min = log_value(fpkm[sam], log).min()
                max = log_value(fpkm[sam], log).max()
                q1 = log_value(fpkm[sam], log).quantile(0.25)
                q3 = log_value(fpkm[sam], log).quantile(0.75)
                median = log_value(fpkm[sam], log).median()
                box[sam] = {"min": min, "max": max, 'q1': q1, 'q3': q3, 'median': median}
                min_q1 = fpkm[[0]][
                    log_value(fpkm[sam], log).apply(lambda x: x >= min and x <= q1)].values
                gene_list[sam]['min-q1'] = [i[0] for i in min_q1]
                q1_median = fpkm[[0]][
                    log_value(fpkm[sam], log).apply(lambda x: x > q1 and x <= median)].values
                gene_list[sam]['q1-median'] = [i[0] for i in q1_median]
                median_q3 = fpkm[[0]][
                    log_value(fpkm[sam], log).apply(lambda x: x > median and x <= q3)].values
                gene_list[sam]['median-q3'] = [i[0] for i in median_q3]
                q3_max = fpkm[[0]][
                    log_value(fpkm[sam], log).apply(lambda x: x > q3 and x <= max)].values
                gene_list[sam]['q3-max'] = [i[0] for i in q3_max]
            return box, gene_list

        express_info = db["sg_express"].find_one({"_id": ObjectId(express_id)})
        files = open(fpkm_path, 'r+')
        samples = files.readline().strip().split("\t")[1:]

        fpkm = pd.read_table(fpkm_path)
        box = {}
        log2box = {}
        log10box = {}
        gene_list = {}
        log2gene_list = {}
        log10gene_list = {}
        # data = [
        # ("express_id", express_id),
        # ("sample_group", sample_group),
        # ("type", query_type)
        # ]

        # data_log2 = data
        # data_log10 = data
        # gene_list_log2 = data
        # gene_list_log10 = data
        box, gene_list = box_info(fpkm=fpkm, samples=samples)
        log2box, log2gene_list = box_info(fpkm=fpkm, log=2, samples=samples)
        log10box, log10gene_list = box_info(fpkm=fpkm, log=10, samples=samples)

        for sam in samples:
            data_log2 = [
                ("express_id", express_id),
                ("sample_group", sample_group),
                ("type", query_type)
            ]
            data_log10 = data_log2

            data_log2 += [
                ('{}_log2'.format(sam), {'{}'.format(sam): log2box[sam]})
            ]
            data_log10 += [
                ('{}_log10'.format(sam), {'{}'.format(sam): log10box[sam]})
            ]
            data_log2 = SON(data_log2)
            data_log10 = SON(data_log10)

            log2_id = db['sg_express_box'].insert_one(data_log2).inserted_id  # 每个样本的box值单独分开导表
            log10_id = db['sg_express_box'].insert_one(data_log10).inserted_id

            data_list_log2 = []
            data_list_log10 = []

            for keys, values in log2gene_list[sam].items():  # 每个样本的不同区段的gene_list分开导表
                insert_data_log2 = [
                    (keys, values),
                    ("express_id", ObjectId(express_id)),
                    ("box_id", ObjectId(log2_id))
                ]
                insert_data_log2 = SON(insert_data_log2)
                data_list_log2.append(insert_data_log2)

            for keys1, values1 in log10gene_list[sam].items():
                insert_data_log10 = [
                    (keys, values),
                    ("express_id", ObjectId(express_id)),
                    ("box_id", ObjectId(log10_id))
                ]
                insert_data_log10 = SON(insert_data_log10)
                data_list_log10.append(insert_data_log10)

            db["sg_express_box_detail"].insert_many(data_list_log2)
            db["sg_express_box_detail"].insert_many(data_list_log10)

            print log2_id, log10_id
            # for sam in samples:
            # data += [
            # (sam, {'{}'.format(sam): box[sam]}),
            # ('{}_gene_list'.format(sam), {'{}'.format(sam): gene_list[sam]})
            # ]
            # data_log2 += [
            # (sam, {'{}_log2'.format(sam): log2box[sam]}),
            # ('{}_gene_list'.format(sam), {'{}_log2'.format(sam): log2gene_list[sam]})
            # ]
            # data_log10 += [
            # (sam, {'{}_log10'.format(sam): log10box[sam]}),
            # ('{}_gene_list'.format(sam), {'{}_log10'.format(sam): log10gene_list[sam]})
            # ]
        # data = SON(data)
        # data_log2 = SON(data_log2)
        # data_log10 = SON(data_log10)
        id = db['sg_express_box'].insert_one(data).inserted_id
        # log2_id = db['sg_express_box'].insert_one(data_log2).inserted_id
        # log10_id = db['sg_express_box'].insert_one(data_log10).inserted_id
        # print log2_id, log10_id

    # @report_check
    def add_express_specimen_detail(self, express_id, rsem_result, rsem_type, sample=None):
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        if not isinstance(express_id, ObjectId):
            if isinstance(express_id, types.StringTypes):
                express_id = ObjectId(express_id)
            else:
                raise Exception('express_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(rsem_result):
            raise Exception('rsem_result所指定的路径：{}不存在，请检查！'.format(rsem_result))
        sample_name = os.path.basename(rsem_result).split('.')[0]
        data_list = []
        with open(rsem_result, 'rb') as f:
            f.readline()
            for line in f:
                line = line.strip().split('\t')
                data = [
                    ('express_id', express_id),
                    ('specimen_name', sample if sample else sample_name),
                    ('type', rsem_type),
                    ('length', float(line[2])),
                    ('effective_length', float(line[3])),
                    ('expected_count', float(line[4])),
                    ('TPM', round(float(line[5]), 4)),
                    ('FPKM', round(float(line[6]), 4)),
                ]
                if rsem_type == 'gene':
                    data += [
                        ('gene_id', line[0]),
                        ('transcript_id', line[1]),
                    ]
                else:
                    data += [
                        ('gene_id', line[1]),
                        ('transcript_id', line[0]),
                        ('IsoPct', float(line[7])),
                    ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = db["sg_express_specimen_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入单样本表达量矩阵：%s信息出错:%s" % (rsem_result, e))
        else:
            self.bind_object.logger.info("导入单样本表达量矩阵: %s信息成功!" % rsem_result)

    def get_diff_list(self, up_down_output, up_down=None, fc=None):
        """
        :param diff_output_path, 差异分析生成的output文件夹，查找结尾是 'results_count'的文件
        :fc 筛选出fa的差异基因/转录本，导基因集的数据也是由此标准筛选之后生成的
        """
        import math
        if not os.path.exists(up_down_output):
            raise Exception("{}文件不存在，无法对up和down差异基因进行分类！".format(up_down_output))
        with open(up_down_output, 'r+') as f1:
            header = f1.readline()
            sequence = []

            for lines in f1:
                line = lines.strip().split("\t")
                seq_id = line[0]
                print seq_id
                print line[-2]
                regulate = line[-2]

                diff_fc = line[-7]  # fc 信息  ###################此处待定可能会有错误
                if fc:
                    standard_fc = math.log(float(fc)) / math.log(2)
                    if diff_fc >= standard_fc:
                        m_ = re.search(regulate, up_down)
                        if m_:
                            sequence.append(seq_id)
                    else:
                        pass
                else:
                    m_ = re.search(regulate, up_down)
                    if m_:
                        sequence.append(seq_id)
                    else:
                        pass
            return sequence

    def add_geneset(self, diff_stat_path, name=None, compare_name=None, group_id=None,
                    express_method=None, type=None, up_down=None):
        """
        添加sg_geneset主表, geneset的名字包括 up 和 down
        """
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn

        if not os.path.exists(diff_stat_path):
            raise Exception('diff_stat_path所指的路径:{}不存在'.fromat(diff_sta_path))
        data_list_up = []
        data_list = []

        with open(diff_stat_path, 'rb') as f:
            head = f.readline().strip().split('\t')
            data_up = {
                'group_id'  : group_id,
                'task_id'   : task_id,
                'desc'      : '%s_vs_%s_差异基因集' % (name, compare_name),
                'project_sn': project_sn,
                'type'      : type,
                "name"      : '{}_vs_{}_{}'.format(name, compare_name, up_down)
            }

        try:
            collection = db["sg_geneset"]
            print collection
            geneset_up_id = collection.insert_one(data_up).inserted_id
            print geneset_up_id
        except Exception, e:
            self.bind_object.logger.error("导入基因表达基因集：%s信息出错:%s" % (diff_stat_path, e))
        else:
            self.bind_object.logger.info("导入基因表达基因集：%s信息成功!" % diff_stat_path)
            return geneset_up_id

    def add_geneset_detail(self, geneset_id, diff_stat_path, fc=None, up_down=None):
        """
        添加sg_geneset_detail表
        """
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        geneset_id = str(geneset_id)
        data_list = []
        data_list_up = []
        up_data = self.get_diff_list(up_down_output=diff_stat_path, up_down=up_down, fc=fc)
        data = [
            ("geneset_id", ObjectId(geneset_id)),
            ("gene_list", up_data)
        ]
        data = SON(data)
        try:
            collection = db["sg_geneset_detail"]
            collection.insert_one(data)
        except Exception, e:
            self.bind_object.logger.error("导入基因集detail表：%s信息出错:%s" % (diff_stat_path, e))
        else:
            self.bind_object.logger.info("导入基因集detail表：%s信息成功!" % diff_stat_path)

    def add_express_diff(self, params, samples, compare_column, compare_column_specimen=None,
                         workflow=True, is_duplicate=None, value_type="fpkm", express_method=None,
                         diff_exp_dir=None, class_code=None, query_type=None, express_id=None,
                         name=None, group_id=None, group_detail=None, control_id=None, major=True):
        # group_id, group_detail, control_id只供denovobase初始化时更新param使用
        """
        差异分析主表
        """
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        params.update({
            'express_id'  : str(express_id),
            'group_id'    : str(group_id),
            'group_detail': group_detail,
            'control_id'  : str(control_id)
        })  # 为更新workflow的params，因为截停
        if group_id == 'all':
            params['group_detail'] = {'all': group_detail}
        insert_data = {
            'project_sn'    : project_sn,
            'task_id'       : task_id,
            'name'          : name if name else 'ExpressDiffStat_' + str(
                datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            'desc'          : '表达量差异检测主表',
            'created_ts'    : datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'params'        : (
            json.dumps(params, sort_keys=True, separators=(',', ':')) if isinstance(params,
                                                                                    dict) else params),
            'specimen'      : samples,
            'status'        : 'end',
            'compare_column': compare_column,
            'group_detail'  : group_detail,
            'express_id'    : express_id,
            "is_duplicate"  : is_duplicate,
            "value_type"    : value_type
        }
        if express_method == 'rsem':
            insert_data["genes"] = True
            insert_data["trans"] = True
        elif express_method.lower() == 'featurecounts':
            insert_data["genes"] = True
            insert_data["trans"] = False
        if compare_column_specimen:
            insert_data["compare_column_specimen"] = compare_column_specimen
        if group_id == 'all':
            insert_data['group_detail'] = {'all': group_detail}
        collection = db['sg_express_diff']
        express_diff_id = collection.insert_one(insert_data).inserted_id
        if major:
            diff_exp_files = os.listdir(diff_exp_dir)
            for f in diff_exp_files:
                if re.search(r'_edgr_stat.xls$', f):
                    con_exp = f.split('_edgr_stat.xls')[0].split('_vs_')
                    name = con_exp[0]
                    compare_name = con_exp[1]
                    self.add_express_diff_detail(express_diff_id, name, compare_name,
                                                 diff_exp_dir + f, workflow, class_code, query_type)
        return express_diff_id

    def get_gene_name(self, class_code, query_type=None, workflow=False):
        """
        :params: 是否工作流根据class_code信息导入基因/转录本名称
        """
        with open(class_code, 'r+') as f1:
            f1.readline()
            data = {}
            for lines in f1:
                line = lines.strip().split("\t")
                if workflow:
                    if query_type == 'transcript':
                        data[line[0]] = line[5]
                    if query_type == 'gene':
                        data[line[1]] = line[5]
                else:
                    data[line[0]] = line[1]
            return data

    def add_express_diff_detail(self, express_diff_id, name, compare_name, diff_stat_path,
                                workflow=False, class_code=None, query_type=None):
        """
        group:为两两比较的样本或分组名，列表
        query_type: gene/transcript
        diff_stat_path: 差异统计表
        workflow: 是否工作流导入文件
        """
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        if not isinstance(express_diff_id, ObjectId):
            if isinstance(express_diff_id, types.StringTypes):
                express_diff_id = ObjectId(express_diff_id)
            else:
                raise Exception('express_diff_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(diff_stat_path):
            raise Exception('diff_stat_path所指定的路径:{}不存在，请检查！'.format(diff_stat_path))
        if class_code:
            if os.path.exists(class_code):
                name_seq_id = self.get_gene_name(class_code, query_type, workflow=workflow)
        data_list = []
        with open(diff_stat_path, 'rb') as f:
            head = f.readline().strip().split('\t')
            for line in f:
                line = line.strip().split('\t')
                data = [
                    ('name', name),
                    ('compare_name', compare_name),
                    ('express_diff_id', express_diff_id),
                ]
                for i in range(len(head)):
                    if i == 0:  # 添加gene_name信息
                        seq_id = line[0]
                        if class_code:
                            if name_seq_id:
                                if seq_id in name_seq_id.keys():
                                    gene_name = name_seq_id[seq_id]
                                    data.append(('gene_name', gene_name))
                                else:
                                    data.append(('gene_name', '-'))
                    if line[i] == 0:
                        data.append((head[i], float(line[i])))
                    elif re.match(r'^(\d+)|.(\d+)$', line[i]) or re.match(r'-(\d+)|.(\d+)$',
                                                                          line[i]):
                        data.append((head[i], float(line[i])))
                    else:
                        data.append((head[i], line[i]))

                # print data
                data = SON(data)
                # print data
                # print "aaaaaaaaaaaaaaaaaaaaaaa"
                data_list.append(data)
            try:
                collection = db["sg_express_diff_detail"]
                collection.insert_many(data_list)
            except Exception, e:
                self.bind_object.logger.error("导入基因表达差异统计表：%s信息出错:%s" % (diff_stat_path, e))
            else:
                self.bind_object.logger.info("导入基因表达差异统计表：%s信息成功!" % diff_stat_path)
                # else:
                # raise Exception("请输入class_code信息！")

    def add_diff_summary_detail(self, diff_express_id, count_path):
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        if not isinstance(diff_express_id, ObjectId):
            if isinstance(diff_express_id, types.StringTypes):
                diff_express_id = ObjectId(diff_express_id)
            else:
                raise Exception('express_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(count_path):
            raise Exception('count_path:{}所指路径不存在'.format(count_path))

        data_list = list()
        count_dict = {}
        with open(count_path, 'rb') as f:
            i = 0
            sample = f.readline().strip().split('\t')
            lensam = len(sample)
            sample = sample[1:lensam]
            for line in f:
                if i == 0:
                    i = 1
                else:
                    l = line.strip().split('\t')
                    gene_id = l[0]
                    alen = len(l)
                    blen = alen - 2
                    alen = alen - 1
                    fpkm = l[1:alen]
                    sum_1 = l[alen]
                    data = [
                        ("seq_id", gene_id),
                        ("express_diff_id", diff_express_id),
                        ('sum', int(sum_1)),
                    ]
                    for j in range(blen):
                        data += [
                            ('{}_diff'.format(sample[j]), fpkm[j]),
                        ]
                    data = SON(data)
                    data_list.append(data)
        try:
            collection = db["sg_express_diff_summary"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入差异分析summary表出错:%s" % e)
        else:
            self.bind_object.logger.info("导入差异分析summary表成功!")

    def add_class_code(self, assembly_method, name=None, major=False, class_code_path=None):
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        data = [
            ('task_id', task_id),
            ('project_sn', project_sn),
            ('assembly_method', assembly_method),
            ('desc', 'class_code信息'),
            ('created_ts', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            ('status', 'end'),
            ('name', name if name else 'Classcode_' + str(
                datetime.datetime.now().strftime("%Y%m%d_%H%M%S")))
        ]
        try:
            collection = db["sg_express_class_code"]
            _id = collection.insert_one(SON(data)).inserted_id
            if major:
                if os.path.exists(class_code_path):
                    self.add_class_code_detail(class_code_path, _id)
        except Exception, e:
            self.bind_object.logger.error("导入class_code表出错:%s" % e)
        else:
            self.bind_object.logger.info("导入class_code表成功！")
            return _id

    def add_class_code_detail(self, class_code, class_code_id):
        db = Config().mongo_client[Config().MONGODB + "_ref_rna"]
        data_list = []
        with open(class_code, 'r+') as f1:
            for lines in f1:
                line = lines.strip().split("\t")
                data = [
                    ('assembly_trans_id', line[0]),
                    ('assembly_gene_id', line[1]),
                    ('class_code', line[2]),
                    ('ref_trans_id', line[3]),
                    ('ref_gene_id', line[4]),
                    ('gene_name', line[5]),
                    ('class_code_id', ObjectId(class_code_id))
                ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = db["sg_express_class_code_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s表出错:%s" % (class_code, e))
        else:
            self.bind_object.logger.info("导入%s表成功！" % (class_code))


if __name__ == "__main__":
    pass

    # db = MongoClient("192.168.10.189:27017").tsanger_ref_rna
    # transcript_fasta_path = "/mnt/ilustre/users/sanger-dev/workspace/20170410/Single_assembly_module_tophat_stringtie_zebra/Assembly/assembly_newtranscripts/merged.fa"
    # # rsem_dir = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_7/Express/output/rsem"
    # # class_code = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_7/Express/output/class_code"
    # rsem_dir = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_9/Express/output/rsem"
    # class_code = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_9/Express/output/class_code"
    # samples=["ERR1621569","ERR1621480","ERR1621658","ERR1621391"]
    # is_duplicate = True
    # params={}
    # params["express_method"]="rsem"
    # params["type"]="fpkm"
    # params["group_id"] = "58f01bbca4e1af488e52de3d"
    # params["group_detail"] = {"A":["58d8a96e719ad0adae70fa14","58d8a96e719ad0adae70fa12"],
    #                            "B":["58d8a96e719ad0adae70fa11", "58d8a96e719ad0adae70fa13"]}
    # distri_path = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_9/Express/MergeRsem"
    # data = RefrnaExpress()
    # data.add_express(rsem_dir=rsem_dir, transcript_fasta_path=transcript_fasta_path, is_duplicate=is_duplicate, class_code = class_code, samples=samples, \
    # params=params, name=None, express_diff_id=None, bam_path=None, major=True, distri_path = distri_path)
    # rsem_dir = "/mnt/ilustre/users/sanger-dev/workspace/20170413/Single_rsem_stringtie_zebra_9/Express/output/oldrsem"

    # add_express(rsem_dir=rsem_dir, transcript_fasta_path=None, is_duplicate=is_duplicate, class_code = None, samples=samples, \
    # params=params, name=None, express_diff_id=, bam_path=None, major=True, distri_path = None)



    # data = db["sg_specimen"].find({"task_id":"tsg_1000"})
    # specimen_info = []
    # samples_A =["ERR1621569","ERR1621480"]
    # samples_B = ["ERR1621658","ERR1621391"] 
    # specimen_add = []
    # for d in data:
    # specimen = d["specimen_name"]
    # if specimen not in specimen_add:
    # specimen_add.append(specimen)
    # specimen_info.append({str(d["_id"]):specimen})
    # print specimen_info
    # print specimen_add
    # opts = {
    # "task_id":"tsg_1000",
    # "category_names":specimen_add,
    # "specimen_names":specimen_info,
    # "group_name":"test",
    # "project_sn":None
    # }
    # opts=SON(opts)
    # group_id = db["sg_specimen_group"].insert_one(opts).inserted_id
    # print group_id
