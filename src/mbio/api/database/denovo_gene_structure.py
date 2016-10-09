# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
from bson.objectid import ObjectId
import datetime
import pandas
import numpy
import json
import glob
import re
import os


class DenovoGeneStructure(Base):
    def __init__(self, bind_object):
        super(DenovoGeneStructure, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_rna'

    @report_check
    def add_orf_table(self, orf_bed, reads_len_info=None, orf_domain=None, name=None, params=None):
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "orf_origin",
            "status": "start",
            "desc": "",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "orf-bed": orf_bed,
        }
        collection = self.db["sg_denovo_orf"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        self.add_orf_bed(orf_bed, inserted_id)
        if orf_domain is not None:
            self.add_orf_domain(orf_domain, inserted_id)
        if reads_len_info is not None:
            self.add_orf_step(reads_len_info, inserted_id)
        return inserted_id

    @report_check
    def add_orf_step(self, reads_len_info, orf_id=None):
        step_files = glob.glob("{}/*".format(reads_len_info))
        data_list = []
        for sf in step_files:
            step = os.path.basename(sf).split(".")[0][5:]
            # self.bind_object.logger.error step
            with open(sf, "r") as f:
                step_line = f.readline().strip().split()
                step_line.pop(0)
                value_line = f.next().strip().split()
                value_line.pop(0)
                col_num = len(step_line)
                value_dict = {}
                for i in range(col_num):
                    value_dict[step_line[i]] = value_line[i]
                # self.bind_object.logger.error value_dict
                data = {
                    "orf_id": orf_id,
                    "step": step,
                    "value": value_dict
                }
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_orf_step"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入ORF长度分布数据出错:%s" % e)
        else:
            self.bind_object.logger.error("导入ORF长度分布数据成功")

    @report_check
    def add_orf_domain(self, domain, orf_id=None):
        data_list = []
        with open(domain, "r") as f:
            f.readline()
            for line in f:
                line = line.strip().split("\t")
                data = {
                    "orf_id": orf_id,
                    "transcript_id": line[0],
                    "gene_id": "_".join(line[0].split("_")[:-1]),
                    "protein_id": line[1],
                    "pfam_id": line[2],
                    "domain": line[3],
                    "domain_desc": line[4],
                    "protein_start": line[5],
                    "protein_end": line[6],
                    "pfam_start": line[7],
                    "pfam_end": line[8],
                    "domain_evalue": line[9]
                }
                data_list.append(data)
        # self.bind_object.logger.error data_list
        try:
            collection = self.db["sg_denovo_orf_domain"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入ORF蛋白域数据出错:%s" % e)
        else:
            self.bind_object.logger.error("导入ORF蛋白域数据成功")

    @report_check
    def add_orf_bed(self, bed, orf_id=None):
        data_list = []
        with open(bed, "r") as f:
            f.readline()
            # self.bind_object.logger.error len(f.next().strip().split())
            for line in f:
                line = line.strip().split()
                data = {
                    "orf_id": orf_id,
                    "transcript_id": line[0],
                    "gene_id": "_".join(line[0].split("_")[:-1]),
                    "trans_start": line[1],
                    "trans_end": line[2],
                    "orf_name": line[3],
                    "score": line[4],
                    "strand": line[5],
                    "orf_start": line[6],
                    "orf_end": line[7],
                    "item_rgb": line[8],
                    "block_count": line[9],
                    "block_size": line[10],
                    "block_starts": line[11]
                }
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_orf_bed"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入ORF预测结果数据出错:%s" % e)
        else:
            self.bind_object.logger.error("导入ORF预测结果数据成功")

    @report_check
    def add_ssr_table(self, ssr, ssr_stat, ssr_primer=None, name=None, params=None):
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "ssr_origin",
            "status": "start",
            "desc": "",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_denovo_ssr"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        self.add_ssr_detail(ssr, inserted_id)
        self.add_ssr_stat(ssr_stat)
        if ssr_primer is not None:
            self.add_ssr_primer(ssr_primer)
        return inserted_id

    @report_check
    def add_ssr_detail(self, ssr, ssr_id=None):
        data_list = []
        with open(ssr, "r") as f:
            f.readline().strip().split("\t")
            # self.bind_object.logger.error f.next().strip().split("\t")
            for line in f:
                line = line.strip().split("\t")
                data = {
                    "ssr_id": ssr_id,
                    "gene_id": line[0],
                    "ssr_nr": line[1],
                    "ssr_type": line[2],
                    "ssr": line[3],
                    "ssr_size": line[4],
                    "ssr_start": line[5],
                    "ssr_end": line[6]
                }
                if len(line) == 8:
                    data["ssr_pos"] = line[7]
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_ssr_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入SSR统计结果数据出错:%s" % e)
        else:
            self.bind_object.logger.error("导入SSR统计结果结果数据成功")

    @report_check
    def add_ssr_primer(self, primer, ssr_id=None):
        data_list = []
        with open(primer, "r") as f:
            f.readline()
            # self.bind_object.logger.error(len(f.next().strip().split("\t")))
            for line in f:
                line = line.strip().split("\t")
                if len(line) < 34:
                    continue
                else:
                    data = {
                        "ssr_id": ssr_id,
                        "gene_id": line[0],
                        "forward1": line[7],
                        "f_tm1": line[8],
                        "f_size1": line[9],
                        "reverse1": line[10],
                        "r_tm1": line[11],
                        "r_size1": line[12],
                        "pro_size1": line[13],
                        "pro_start1": line[14],
                        "pro_end1": line[15],
                        "forward2": line[16],
                        "f_tm2": line[17],
                        "f_size2": line[18],
                        "reverse2": line[19],
                        "r_tm2": line[20],
                        "r_size2": line[21],
                        "pro_size2": line[22],
                        "pro_start2": line[23],
                        "pro_end2": line[24],
                        "forward3": line[25],
                        "f_tm3": line[26],
                        "f_size3": line[27],
                        "reverse3": line[28],
                        "r_tm3": line[29],
                        "r_size3": line[30],
                        "pro_size3": line[31],
                        "pro_start3": line[32],
                        "pro_end3": line[33],
                    }
                    data_list.append(data)
        try:
            collection = self.db["sg_denovo_ssr_primer"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入SSR引物结果数据出错:%s" % e)
        else:
            self.bind_object.logger.error("导入SSR引物结果结果数据成功")

    @report_check
    def add_ssr_stat(self, ssr_stat, ssr_id=None):
        data_list = []
        target_line = False
        with open(ssr_stat, "r") as f:
            for n, line in enumerate(f):
                if re.match(r"Frequency of classified", line):
                    target_line = True
                    f.next()
                    f.next()
                    continue
                elif target_line:
                    line = line.strip().split('\t')
                    bar_a = line[1:12]
                    bar_b = line[13:47]
                    value_a = 0
                    value_b = 0
                    for a in bar_a:
                        if a == "":continue
                        if a == "-":continue
                        else:value_a += int(a)
                    for b in bar_b:
                        if b == "":continue
                        if b == "-":continue
                        else:value_b += int(b)
                    data = {
                        "ssr_id": ssr_id,
                        "ssr_type": line[0],
                        "5-15": value_a,
                        "16-51": value_b,
                        "total": line[48]
                    }
                    data_list.append(data)
        try:
            collection = self.db["sg_denovo_ssr_stat"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入SSR引物统计数据出错:%s" % e)
        else:
            self.bind_object.logger.error("导入SSR引物统计数据成功")

    @report_check
    def add_snp_table(self, snp, name=None, params=None):
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "snp_origin",
            "status": "start",
            "desc": "",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_denovo_snp"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        self.add_snp_detail(snp, inserted_id)
        self.add_snp_graph(snp, inserted_id)
        return inserted_id

    @report_check
    def add_snp_detail(self, snp, snp_id=None):
        snp_files = glob.glob("{}/*snp.xls".format(snp))
        # self.bind_object.logger.error snp_files
        data_list = []
        for sf in snp_files:
            sample_name = os.path.basename(sf).split(".")[0]
            # self.bind_object.logger.error(sample_name)
            with open(sf, "r") as f:
                f.readline()
                # self.bind_object.logger.error f.next().strip().split()
                # self.bind_object.logger.error len(f.next().strip().split("\t"))
                for line in f:
                    line = line.strip().split("\t")
                    data = {
                        "snp_id": snp_id,
                        "specimen_name": sample_name,
                        "gene_id": line[0],
                        "nucl_pos": line[1],
                        "ref": line[2],
                        "cous": line[3],
                        "reads1_num": line[4],
                        "reads2_num": line[5],
                        "var_freq": line[6],
                        "var_allele": line[18]
                    }
                    if len(line) == 20:
                        data["gene_pos"] = line[19]
                    data_list.append(data)
        try:
            collection = self.db["sg_denovo_snp_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入SSR引物统计数据出错:%s" % e)
        else:
            self.bind_object.logger.error("导入SSR引物统计数据成功")

    @report_check
    def add_snp_graph(self, snp, snp_id=None):
        snp_pos = glob.glob("{}/*position_stat.xls".format(snp))
        snp_type = glob.glob("{}/*type_stat.xls".format(snp))
        data_list = []
        for sp in snp_pos:
            sample_name = os.path.basename(sp).split(".")[0]
            data = {
                "snp_id": snp_id,
                "specimen_name": sample_name
            }
            with open(sp, "r") as f:
                f.readline()
                pos_value = {}
                for line in f:
                    line = line.strip().split("\t")
                    pos_value[line[0]] = line[1]
                data["pos_stat"] = pos_value
                data_list.append(data)
        for st in snp_type:
            sample_name = os.path.basename(st).split(".")[0]
            with open(st, "r") as f:
                f.readline()
                type_value = {}
                for line in f:
                    line = line.strip().split("\t")
                    type_value[line[0]] = line[1]
                for data in data_list:
                    if sample_name == data["specimen_name"]:
                        data["type_stat"] = type_value
        try:
            collection = self.db["sg_denovo_snp_graphic"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入SSR引物统计数据出错:%s" % e)
        else:
            self.bind_object.logger.error("导入SSR引物统计数据成功")
