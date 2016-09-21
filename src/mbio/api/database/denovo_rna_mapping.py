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


class DenovoRnaMapping(Base):
    def __init__(self, bind_object):
        super(DenovoRnaMapping, self).__init__(bind_object)
        self._db_name = Config().MONGODB

    @report_check
    def add_mapping_stat(self, stat_file):
        with open(stat_file, "r") as f:
            data_list = []
            f.readline()
            for line in f:
                line = line.strip().split()
                data = {
                    "project_sn": self.bind_object.sheet.project_sn,
                    "task_id": self.bind_object.sheet.project_sn,
                    "specimen_name": line[0],
                    "mapping_reads": line[1],
                    "mapping_rate": line[2]
                }
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_specimen_mapping"]
            collection.insert_many(data_list)
        except Exception, e:
            print("导入比对结果统计信息出错:%s" % e)
        else:
            print("导入比对结果统计信息成功")

    @report_check
    def add_rpkm_table(self, file_path, name=None, params=None):
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "rpkm_origin",
            "status": "start",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_denovo_rpkm"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        self.add_rpkm_detail(inserted_id, file_path)
        return inserted_id

    @report_check
    def add_rpkm_table(self, file_path, name=None, params=None):
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "rpkm_origin",
            "status": "start",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "curve_specimen": {"column1": "[0-0.3)", "column2": "[0.3-0.6)", "column3": "[0.6-3.5)", "column4": "[3.5-15)", "column5": "[15-60)", "column6": ">=60"},
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_denovo_rpkm"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        self.add_rpkm_detail(file_path, inserted_id)
        self.add_rpkm_box(file_path, inserted_id)
        self.add_rpkm_curve(file_path, inserted_id)
        return inserted_id

    @report_check
    def add_rpkm_detail(self, rpkm_file, rpkm_id=None):
        rpkm_tables = glob.glob("{}/*eRPKM.xls".format(rpkm_file))
        rpkm_detail = []
        for rt in rpkm_tables:
            sample_name = os.path.basename(rt).split(".")[0][6:]
            with open(rt, "r") as f:
                column_name = f.readline().strip().split()[6:]
                for line in f:
                    line = line.strip().split()
                    col_num = 6
                    data = {
                        "rpkm_id": rpkm_id,
                        "specimen_name": sample_name,
                        # "specimen_id" : get_sample_id(sample_name, "after"),
                        "transcript_id": line[0]
                    }
                    for col in column_name:
                        data["{}".format(col)] = line[col_num]
                        col_num += 1
                    rpkm_detail.append(data)
        try:
            collection = self.db["sg_denovo_rpkm_detail"]
            collection.insert_many(rpkm_detail)
        except Exception, e:
            print("导入rpkm detail出错:%s" % e)
        else:
            print("导入rpkm detail成功")

    def add_rpkm_box(self, rpkm_file, rpkm_id=None):
        rpkm_plot = glob.glob("{}/*saturation.r".format(rpkm_file))
        rpkm_box = []
        for rp in rpkm_plot:
            sample_name = os.path.basename(rp).split(".")[0][6:]
            sam_dict = {}
            with open(rp, "r") as f:
                for line in f:
                    if re.match(r"S", line):
                        split_line = line.strip().split("=c(")
                        sampling_percent = split_line[0][1:]
                        box_data = split_line[1][:-1].split(",")
                        box_data = map(float, box_data)
                        data = pandas.DataFrame({"name": box_data})
                        result = data.boxplot(return_type='dict')
                        qualities = {
                            "q1": result['whiskers'][0].get_data()[1][0],
                            "mean": numpy.mean(box_data, axis=0),
                            "q3": result['whiskers'][1].get_data()[1][0],
                            "max": max(box_data),
                            "min": min(box_data)
                            }
                        if sampling_percent in sam_dict:
                            sam_dict[sampling_percent].append(qualities)
                        else:
                            sam_dict[sampling_percent] = [qualities]
            for sam in sam_dict:
                data = {
                    "rpkm_id": rpkm_id,
                    "specimen_name": sample_name,
                    "sampling": sam,
                    "Q1": sam_dict[sam][0],
                    "Q2": sam_dict[sam][1],
                    "Q3": sam_dict[sam][2],
                    "Q4": sam_dict[sam][3]
                }
                rpkm_box.append(data)
        try:
            collection = self.db["sg_denovo_rpkm_box"]
            collection.insert_many(rpkm_box)
        except Exception, e:
            print("导入rpkm箱线图数据出错:%s" % e)
        else:
            print("导入rpkm箱线图数据")

    @report_check
    def add_rpkm_curve(self, rpkm_file, rpkm_id=None):
        curve_files = glob.glob("{}/*cluster_percent.xls".format(rpkm_file))
        curve_data = []
        for cf in curve_files:
            sample_name = os.path.basename(cf).split(".")[0][6:]
            with open(cf, "r") as f:
                line_list = []
                for line in f:
                    line = line.strip().split()
                    line.pop(0)
                    line_list.append(line)
                data = {
                    "rpkm_id": rpkm_id,
                    "specimen_name": sample_name,
                    "column1": line_list[0],
                    "column2": line_list[1],
                    "column3": line_list[2],
                    "column4": line_list[3],
                    "column5": line_list[4],
                    "column6": line_list[5],
                }
                # print data
                curve_data.append(data)
        try:
            collection = self.db["sg_denovo_rpkm_curve"]
            collection.insert_many(curve_data)
        except Exception, e:
            print("导入rpkm曲线数据出错:%s" % e)
        else:
            print("导入rpkm曲线数据成功")

    @report_check
    def add_coverage_table(self, coverage, name=None, params=None):
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "coverage_origin",
            "status": "start",
            "desc": "",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_denovo_coverage"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        self.add_coverage_detail(coverage, inserted_id)
        return inserted_id

    @report_check
    def add_coverage_detail(self, coverage, coverage_id=None):
        coverage_files = glob.glob("{}/*geneBodyCoverage.txt".format(coverage))
        data_list = []
        for cf in coverage_files:
            sample_name = os.path.basename(cf).split(".")[0][9:]
            # print sample_name
            with open(cf, "r") as f:
                percent = f.readline().strip().split()
                value = f.next().strip().split()
                plot_value = {}
                for i in range(100):
                    plot_value[percent[i+1]] = value[i+1]
                # print plot_value
                data = {
                    "coverage_id": coverage_id,
                    "specimen_name": sample_name,
                    "plot_value": plot_value
                }
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_coverage_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            print("导入rpkm曲线数据出错:%s" % e)
        else:
            print("导入rpkm曲线数据成功")

    @report_check
    def add_duplication_table(self, dup, name=None, params=None):
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "duplication_origin",
            "status": "start",
            "desc": "",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_denovo_duplicate"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        self.add_duplication_detail(dup, inserted_id)
        return inserted_id

    @report_check
    def add_duplication_detail(self, dup, dup_id=None):
        dup_seqs = glob.glob("{}/*seq.DupRate.xls".format(dup))
        dup_poss = glob.glob("{}/*pos.DupRate.xls".format(dup))
        data_list = []
        for dp in dup_seqs:
            sample_name = os.path.basename(dp).split(".")[0][4:]
            with open(dp, "r") as f:
                f.readline()
                map_point = {}
                for line in f:
                    line = line.strip().split()
                    map_point[line[0]] = line[1]
                data = {
                    "dup_id": dup_id,
                    "specimen_name": sample_name,
                    "map_point": map_point
                }
                data_list.append(data)

        for ds in dup_poss:
            sample_name = os.path.basename(ds).split(".")[0][4:]
            with open(ds, "r") as f:
                f.readline()
                seq_point = {}
                for line in f:
                    line = line.strip().split()
                    seq_point[line[0]] = line[1]
            for dl in data_list:
                if sample_name == dl["specimen_name"]:
                    dl["seq_point"] = seq_point
        try:
            collection = self.db["sg_denovo_duplicate_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            print("导入冗余分析数据出错:%s" % e)
        else:
            print("导入冗余分析数据成功")

    @report_check
    def add_correlation_table(self, correlation, name=None, params=None):
        correlation_tree = glob.glob("{}/*.tre".format(correlation))
        with open(correlation_tree[0], "r") as t:
            correlation_tree = t.readline().strip()
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "correlation_origin",
            "status": "start",
            "desc": "",
            "correlation_tree": correlation_tree,
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_denovo_correlation"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        self.add_correlation_detail(correlation, inserted_id)
        return inserted_id

    @report_check
    def add_correlation_detail(self, collection, correlation_id=None):
        correlation_matrix = collection + "/correlation_matrix.xls"
        data_list = []
        with open(correlation_matrix, "r") as m:
            samples = m.readline().strip().split()
            for line in m:
                data = {
                    "correlation_id": correlation_id
                }
                line = line.strip().split()
                for i, s in enumerate(samples):
                    data["specimen_name"] = line[0]
                    data[s] = line[i+1]
                # print data
                data_list.append(data)
        try:
            collection = self.db["sg_denovo_correlation_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            print("导入相关系数分析数据出错:%s" % e)
        else:
            print("导入相关系数分析数据成功")