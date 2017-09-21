# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from __future__ import division
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
from bson.objectid import ObjectId
# from cStringIO import StringIO
# import bson.binary
import datetime
# import pandas
# import numpy
from bson.son import SON
import json
import glob
import re
import os


class RefRnaQc(Base):
    def __init__(self, bind_object):
        super(RefRnaQc, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'

    @report_check
    def add_samples_info(self, qc_stat, qc_adapt=None, fq_type='se', about_qc='before'):
        """
        :param qc_stat: 统计结果文件夹，即module.output_dir
        :param qc_adapt:去接头率文件，由于需求变动，可不传
        :param fq_type:测序类型
        :return:
        """
        stat_file = qc_stat + "/fastq_stat.xls"
        dup_file = qc_stat + "/dup.xls"
        dup = ""
        dup_rate = {}
        adapter = False
        if not os.path.exists(stat_file):
            raise Exception("%s文件不存在" % stat_file)
        if qc_adapt is not None:
            adapt_rate = {}
            adapter = True
            with open(qc_adapt, "r") as a:
                for line in a:
                    line = line.split()
                    adapt_rate[line[0]] = line[1]
        if os.path.exists(dup_file):
            with open(dup_file, "r") as d:
                col_num = len(d.readline().split())
                if col_num == 4:
                    dup = "pe"
                    for line in d:
                        line = line.split()
                        dup_rate[line[0]] = [float(line[1]), float(line[2]), float(line[3])]
                if col_num == 2:
                    dup = "se"
                    for line in d:
                        line = line.split()
                        dup_rate[line[0]] = line[1]
        with open(stat_file, "r") as f:
            data_list = []
            first_line = f.readline()
            if not re.match(r"#Sample_ID", first_line):
                raise Exception("%s文件类型不正确" % stat_file)
            for line in f:
                line = line.split()
                data = {
                        "project_sn": self.bind_object.sheet.project_sn,
                        "task_id": self.bind_object.sheet.id,
                        "specimen_name": line[0],
                        "total_reads": int(line[1]),
                        "total_bases": int(line[2]),
                        "reads_with_ns": int(line[3]),
                        "n_reads_rate": float(line[4]),
                        "a_rate": float(line[5]),
                        "t_rate": float(line[6]),
                        "c_rate": float(line[7]),
                        "g_rate": float(line[8]),
                        "n_rate": float(line[9]),
                        "error_rate": float(line[10]),
                        "q20_rate": float(line[11]),
                        "q30_rate": float(line[12]),
                        "gc_rate": float(line[13]),
                        "about_qc": about_qc,
                        "type": fq_type   # 怎么得知待定
                        }
                if line[0] in dup_rate:
                    if dup == "se":
                        data["reads1_dup_rate"] = dup_rate[line[0]]
                    if dup == "pe":
                        data["reads1_dup_rate"] = dup_rate[line[0]][0]
                        data["reads2_dup_rate"] = dup_rate[line[0]][1]
                        data["paired_dup_rate"] = dup_rate[line[0]][1]
                if adapter:
                    if line[0] in adapt_rate:
                        data["adapt_rate"] = adapt_rate[line[0]]
                        data["about_qc"] = "after"
                data_list.append(data)

            try:
                collection = self.db["sg_specimen"]
                result = collection.insert_many(data_list)
            except Exception, e:
                self.bind_object.logger.error("导入样品信息数据出错:%s" % e)
            else:
                self.bind_object.logger.info("导入样品信息数据成功")
                self.sample_ids = result.inserted_ids
        sample_ids = [str(i) for i in result.inserted_ids]
        return sorted(sample_ids)

    @report_check
    def add_gragh_info(self, quality_stat, about_qc="before"):
        """
        :param quality_stat: ouput_dir里一个叫qualityStat的文件夹，即~/output_dir/qualityStat
        :param about_qc:指控后的或是质控前的统计，质控前统计为before，指控后传after
        :return:
        """
        stat_files = glob.glob("{}/*".format(quality_stat))
        data_list = []
        for sf in stat_files:
            sample_name = os.path.basename(sf).split(".")[0]
            self.bind_object.logger.info('%s,%s' % (sf, sample_name))
            spname_spid = self.get_spname_spid()
            site = os.path.basename(sf).split(".")[1]
            if site == "l": site_type = "left"
            elif site == "r": site_type = "right"
            else: site_type = "single"
            with open(sf, "r") as f:
                f.readline()
                for line in f:
                    line = line.strip().split()
                    data = {
                        "project_sn": self.bind_object.sheet.project_sn,
                        "task_id": self.bind_object.sheet.id,
                        "specimen_name": sample_name,
                        "specimen_id": spname_spid[sample_name],
                        "type": site_type,
                        "about_qc": about_qc,
                        "column": int(line[0]),
                        "min": int(line[10]),
                        "max": int(line[11]),
                        "q1": int(line[6]),
                        "q3": int(line[8]),
                        "median": int(line[7]),
                        "error": 10 ** (float(line[5])/(-10)) * 100,
                        "A": int(line[12])/int(line[17]) * 100,
                        "T": int(line[15])/int(line[17]) * 100,
                        "C": int(line[13])/int(line[17]) * 100,
                        "G": int(line[14])/int(line[17]) * 100,
                        "N": int(line[16])/int(line[17]) * 100
                    }
                    data_list.append(data)
        try:
            collection = self.db["sg_specimen_graphic"]
            result = collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入样品画图数据信息出错:%s" % e)
        else:
            self.bind_object.logger.info("导入样品画图数据信息成功")

    @report_check
    def add_specimen_group(self, file):
        """
        group_list: group_list为一个字典, 字典的键为分组名称，值为分组中所带样本名
        specimen_names: 数组形式，数组中的每一项为一个字典，与分组相对应，字典的键为样本id，值为样本名
                ex:[
            {
                "59473300a4e1af65bfaf2d76" : "CL1",
                "59473300a4e1af65bfaf2d77" : "CL2",
                "59473300a4e1af65bfaf2d74" : "HFL3",
                "59473300a4e1af65bfaf2d75" : "CL5"
            },
            {
                "59473300a4e1af65bfaf2d6f" : "HGL4",
                "59473300a4e1af65bfaf2d72" : "HFL6",
                "59473300a4e1af65bfaf2d73" : "HFL4",
                "59473300a4e1af65bfaf2d70" : "HGL3",
                "59473300a4e1af65bfaf2d71" : "HGL1"
            }
        ]
        group_sorted: 数组形式， 数组中的每一项为一个分组: ["A", "B"]
        :param file: group_table文件
        :return:
        """
        spname_spid = self.get_spname_spid()
        group_list = dict()
        with open(file, "r") as f:
            f.readline()
            for line in f:
                tmp = line.strip().split("\t")
                group_name = tmp[1]
                if group_name not in group_list.keys():
                    group_list[group_name] = [tmp[0]]
                else:
                    group_list[group_name].append(tmp[0])
        spcecimen_names = list()
        group_sorted = list()
        for key in group_list.keys():
            lst = group_list[key]
            tmp_dict = dict()
            for sample in lst:
                sp_id = spname_spid[sample]
                tmp_dict[str(sp_id)] = sample
            spcecimen_names.append(tmp_dict)
            group_sorted.append(key)
        data = {
            "task_id" : self.bind_object.sheet.id,
            "category_names": group_sorted,
            "specimen_names": spcecimen_names,
            "group_name": os.path.basename(file),
            "project_sn": self.bind_object.sheet.project_sn
        }
        col = self.db["sg_specimen_group"]
        group_id = col.insert_one(data).inserted_id
        self.bind_object.logger.info("导入样本分组信息成功")
        return group_id, spcecimen_names, group_sorted

    @report_check
    def add_bam_path(self, dir_path):
        """
        将bam文件的路径插入sg_specimen表中，供可变剪切使用
        :param dir_path:传入的rnaseq_mapping的output_dir
        :return:
        """
        spname_spid = self.get_spname_spid()
        col = self.db["sg_specimen"]
        for spname in spname_spid:
            sp_id = str(spname_spid[spname])
            bam_path = dir_path + "/bam/" + spname + ".bam"
            insert_data = {"bam_path": bam_path}
            col.find_one_and_update({"task_id": sp_id}, {"$set": insert_data})

    @report_check
    def add_control_group(self,file, group_id):
        con_list = list()
        with open(file, "r") as f:
            f.readline()
            for line in f:
                tmp = line.strip().split("\t")
                string = tmp[0] + "|" + tmp[1]
                con_list.append(string)
        col = self.db["sg_specimen_group"]
        result = col.find_one({"_id": group_id})
        group_name = result["group_name"]
        category_names = str(result["category_names"])
        data = {
            "task_id": self.bind_object.sheet.id,
            "compare_group_name": group_name,
            "compare_names": json.dumps(con_list),
            "compare_category_name": "all",
            "specimen_group_id": str(group_id)
        }
        col = self.db["sg_specimen_group_compare"]
        try:
            com_id = col.insert_one(SON(data)).inserted_id
        except Exception,e:
            self.bind_object.logger.error("导入样本对照组信息出错:%s" % e)
        else:
            self.bind_object.logger.info("导入样本对照组信息成功")
            return com_id, con_list

    @report_check
    def get_spname_spid(self):
        if not self.sample_ids:
            raise Exception("样本id列表为空，请先调用add_samples_info产生sg_speciem的id")
        collection = self.db["sg_specimen"]
        spname_spid = {}
        for id_ in self.sample_ids:
            results = collection.find_one({"_id": id_})
            spname_spid[results['specimen_name']] = id_
        return spname_spid

    @report_check
    def add_mapping_stat(self, stat_file, type="genome"):
        """
        :param stat_file: 比对结果统计文件，在mapassessment module的ouput_dir中，即~/MapAssessment/output/bam_stat.xls
        :param type: 比对到基因组的传genome，比对到基因的传gene
        :return:
        """
        with open(stat_file, "r") as f:
            data_list = []
            f.readline()
            for line in f:
                line = line.strip().split()
                data = {
                    "project_sn": self.bind_object.sheet.project_sn,
                    "task_id": self.bind_object.sheet.id,
                    "type": type,
                    "specimen_name": line[0],
                    "total_reads": line[1],
                    "mapping_reads": line[2] + "(" + str(float("%0.4f" % (float(line[2])/float(line[1])))*100) + "%" + ")",
                    "multiple_mapped": line[3] + "(" + str(float("%0.4f" % (float(line[3])/float(line[1])))*100) + "%" + ")",
                    "uniq_mapped": line[4] + "(" + str(float("%0.4f" % (float(line[4])/float(line[1])))*100) + "%" + ")",
                    "map_to_up": line[5] + "(" + str(float("%0.4f" % (float(line[5])/float(line[1])))*100) + "%" + ")",
                    "map_to_down": line[6] + "(" + str(float("%0.4f" % (float(line[6])/float(line[1])))*100) + "%" + ")",
                    "non_splice_reads": line[7] + "(" + str(float("%0.4f" % (float(line[7])/float(line[1])))*100) + "%" + ")",
                    "splice_reads": line[8] + "(" + str(float("%0.4f" % (float(line[8])/float(line[1])))*100) + "%" + ")",
                    "mapping_rate": str(float("%0.4f" % (float(line[2])/float(line[1])))*100) + "%",
                    # "multiple_mapped": line[3],
                    "multiple_rate": str(float("%0.4f" % (float(line[3])/float(line[1])))*100) + "%",
                    # "uniq_mapped": line[4],
                    "uniq_rate": str(float("%0.4f" % (float(line[4])/float(line[1])))*100) + "%"
                }
                data_list.append(data)
        # print data_list
        try:
            collection = self.db["sg_specimen_mapping"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入比对结果统计信息出错:%s" % e)
        else:
            self.bind_object.logger.info("导入比对结果统计信息成功")

    @report_check
    def add_rpkm_table(self, file_path, name=None, params=None, detail=True):
        """
        :param file_path: 文件夹，即~/MapAssessment/output/satur
        :param name: 主表的名称，可不传
        :param params: 参数，可不传
        :param detail: 如果需要主表跟detail表一起导入传true,否则false
        :return:
        """
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "saturation_origin",
            "status": "end",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "curve_category": ['5', '10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '60', '65', '70', '75', '80', '85', '90', '95', '100'],
            "curve_specimen": {"column1": "[0-0.3)", "column2": "[0.3-0.6)", "column3": "[0.6-3.5)", "column4": "[3.5-15)", "column5": "[15-60)", "column6": ">=60"},
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_assessment_saturation"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        if detail:
            self.add_rpkm_curve(file_path, inserted_id)
        return inserted_id

    @report_check
    def add_rpkm_curve(self, rpkm_file, rpkm_id=None):
        """
        :param rpkm_file:文件夹，即~/MapAssessment/output/satur
        :param rpkm_id:主表id，必传
        :return:
        """
        rpkm_id = ObjectId(rpkm_id)
        curve_files = glob.glob("{}/*cluster_percent.xls".format(rpkm_file))
        # curve_data = []
        R_files = glob.glob("{}/*eRPKM.xls.saturation.R".format(rpkm_file))
        # print R_files
        sample_categaries = {}
        for rf in R_files:
            sample_name = os.path.basename(rf).split(".")[0][6:]
            categaries = self.add_satur_count(rf)
            sample_categaries[sample_name] = categaries
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
                    "saturation_id": rpkm_id,
                    "specimen_name": sample_name,
                    "categories": sample_categaries[sample_name],
                    "column1": line_list[0],
                    "column2": line_list[1],
                    "column3": line_list[2],
                    "column4": line_list[3],
                    "column5": line_list[4],
                    "column6": line_list[5]
                }
                # self.bind_object.logger.error data
                curve_data.append(data)
        try:
            collection = self.db["sg_assessment_saturation_curve"]
            collection.insert_many(curve_data)
        except Exception, e:
            self.bind_object.logger.error("导入rpkm曲线数据出错:%s" % e)
        else:
            self.bind_object.logger.info("导入rpkm曲线数据成功")

    @report_check
    def add_satur_count(self, count_r_file):
        categaries = {"column1": "[0-0.3)", "column2": "[0.3-0.6)", "column3": "[0.6-3.5)", "column4": "[3.5-15)", "column5": "[15-60)", "column6": ">=60"}
        with open(count_r_file, "r") as f:
            for line in f:
                if re.match(r"legend", line):
                    all_num = re.findall("num=[\d]*", line)
                    # print all_num
                    categaries = {
                        "column5": "[15-60)=" + all_num[4][4:],
                        "column4": "[3.5-15)=" + all_num[3][4:],
                        "column6": ">=60=" + all_num[5][4:],
                        "column1": "[0-0.3)=" + all_num[0][4:],
                        "column3": "[0.6-3.5)=" + all_num[2][4:],
                        "column2": "[0.3-0.6)=" + all_num[1][4:]
                    }
        return categaries

    @report_check
    def add_coverage_table(self, coverage, name=None, params=None, detail=True):
        """
        :param coverage: 文件夹，即~/MapAssessment/output/coverage
        :param name: 主表名称。可不传
        :param params:参数，可不传
        :param detail:如果需要主表跟detail表一起导入传true,否则false
        :return:
        """
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "coverage_origin",
            "status": "end",
            "desc": "",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_assessment_coverage"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        if detail:
            self.add_coverage_detail(coverage, inserted_id)
        return inserted_id

    @report_check
    def add_coverage_detail(self, coverage, coverage_id=None):
        """
        :param coverage: 文件夹，即~/MapAssessment/output/coverage
        :param coverage_id: 主表ID，必传
        :return:
        """
        coverage_files = glob.glob("{}/*geneBodyCoverage.txt".format(coverage))
        data_list = []
        for cf in coverage_files:
            sample_name = os.path.basename(cf).split(".")[0][9:]
            # self.bind_object.logger.error sample_name
            with open(cf, "r") as f:
                percent = f.readline().strip().split()
                value = f.next().strip().split()
                plot_value = {}
                for i in range(100):
                    plot_value[percent[i+1]] = value[i+1]
                data = {
                    "coverage_id": coverage_id,
                    "specimen_name": sample_name,
                    "plot_value": plot_value
                }
                data_list.append(data)
        try:
            collection = self.db["sg_assessment_coverage_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入rpkm曲线数据出错:%s" % e)
        else:
            self.bind_object.logger.info("导入rpkm曲线数据成功")

    @report_check
    def add_distribution_table(self, distribution=None, name=None, params=None):
        """
        :param distribution: 文件夹，即~/MapAssessment/output/distribution,不传的时候只导主表
        :param name: 主表名称。可不传
        :param params:参数，可不传
        :return:
        """
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "distribution_origin",
            "status": "end",
            "desc": "",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_assessment_distribution"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        if distribution:
            self.add_distribution_detail(distribution, inserted_id)
        return inserted_id

    @report_check
    def add_distribution_detail(self, distribution, distribution_id):
        data_list = []
        stat_files = glob.glob("{}/*reads_distribution.txt".format(distribution))
        distributions = ["cds", "utr5", "utr3", "introns", "intergenic"]
        for fls in stat_files:
            sample_name = os.path.basename(fls).split(".")[0]
            values = []
            with open(fls, "r") as f:
                f.readline()
                f.next()
                f.next()
                f.next()
                f.next()
                data = {
                    "distribution_id": distribution_id,
                    "specimen_name": sample_name
                }
                for line in f:
                    if re.match(r"==", line):
                        continue
                    else:
                        line = line.strip().split()
                        values.append(float(line[2]))
                # print values
                # print sum(values[4:])
                values_new = values[:4]
                values_new.append(sum(values[4:]))
                # print values_new
                total = sum(values_new)
                for n, dis in enumerate(distributions):
                    # data[dis] = values_new[n]
                    data[dis] = str(values_new[n]) + "(" + str(float("%0.4f" % (values_new[n] / total)) * 100) + "%)"
                print data
                data_list.append(data)
            # print data_list
        try:
            collection = self.db["sg_assessment_distribution_detail"]
            result = collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.info("导入reads distribution详情表出错:%s" % e)
        else:
            self.bind_object.logger.info("导入reads distribution详情表成功")

    @report_check
    def add_chorm_distribution_table(self, distribution=None, name=None, params=None):
        """
        :param distribution: 文件夹，即~/MapAssessment/output/chr_stat,不传的时候只导主表
        :param name: 主表名称。可不传
        :param params:参数，可不传
        :return:
        """
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": name if name else "distribution_origin",
            "status": "end",
            "desc": "",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_assessment_chrom_distribution"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        if distribution:
            self.add_chorm_distribution_detail(distribution, inserted_id)
        return inserted_id

    @report_check
    def add_chorm_distribution_detail(self, chorm_stat, chorm_stat_id):
        stat_files = glob.glob("{}/*.bam_chr_stat.xls".format(chorm_stat))
        data_list = []
        distribution = set()
        specimen_names = []
        for fls in stat_files:
            chrs = []
            # chr_values = []
            sample_name = os.path.basename(fls).split(".")[0]
            specimen_names.append(sample_name)
            with open(fls, "r") as f:
                data = {
                    "chrom_distribution_id": chorm_stat_id,
                    "specimen_name": sample_name,
                    "chr_values": []
                }
                for line in f:
                    values = {}
                    if re.match(r"#", line):
                        continue
                    else:
                        line = line.strip().split()
                        chrs.append(line[0])
                        distribution.add(line[0])
                        values["chr_name"] = line[0]
                        values["value"] = line[1]
                        data["chr_values"].append(values)
                # print data
                data_list.append(data)
                # print chrs
        try:
            collection = self.db["sg_assessment_chrom_distribution_detail"]
            result = collection.insert_many(data_list)
            main_collection = self.db["sg_assessment_chrom_distribution"]
            main_collection.update({"_id": ObjectId(chorm_stat_id)}, {"$set": {"distribution": list(distribution), "specimen_name": specimen_names}})
        except Exception, e:
            self.bind_object.logger.info("导入sg_assessment_chrom_distribution_detail出错:%s" % e)
        else:
            self.bind_object.logger.info("导入sg_assessment_chrom_distribution_detail成功")

    @report_check
    def add_tophat_mapping_stat(self, stat_file):
        files = glob.glob("{}/*".format(stat_file))
        data_list = []
        for fs in files:
            specimen_name = os.path.basename(fs).split(".")[0]
            print specimen_name
            f = open(fs, "r")
            data = {
                    "project_sn": self.bind_object.sheet.project_sn,
                    "task_id": self.bind_object.sheet.id,
                    "type": "genome",
                    "specimen_name": specimen_name
            }
            total_reads = 0
            map_reads = 0
            multiple = 0
            for line in f:
                # print map_reads
                if re.match(r"Left", line):
                    total_reads += int(f.next().split()[-1])
                    map_reads += int(f.next().split()[2])
                    multiple += int(f.next().split()[2])
                if re.match(r"Right", line):
                    total_reads += int(f.next().split()[-1])
                    map_reads += int(f.next().split()[2])
                    multiple += int(f.next().split()[2])
            print total_reads, map_reads, multiple, total_reads - multiple
            data["total_reads"] = total_reads
            # print "(" + str(float("%0.4f" % ( map_reads/total_reads)) * 100) + "%" + ")"
            data["mapping_reads"] = str(map_reads) + "(" + str(float("%0.4f" % ( map_reads/total_reads)) * 100) + "%" + ")"
            data["multiple_mapped"] = str(multiple) + "(" + str(float("%0.4f" % ( multiple/total_reads)) * 100) + "%" + ")"
            data["uniq_mapped"] = str(total_reads - multiple) + "(" + str(float("%0.4f" % ((total_reads - multiple)/total_reads)) * 100) + "%" + ")"
            # print data
            data_list.append(data)
            f.close()
        try:
            collection = self.db["sg_specimen_mapping"]
            collection.insert_many(data_list)
        except Exception, e:
            print("导入比对结果统计信息出错:%s" % e)
        else:
            print("导入比对结果统计信息成功")

    @report_check
    def add_hisat_mapping_stat(self, stat_file):
        files = glob.glob("{}/*".format(stat_file))
        # print files
        data_list = []
        for fs in files:
            specimen_name = os.path.basename(fs).split(".")[0]
            # print specimen_name
            values = []
            f = open(fs, "r")
            for line in f:
                # print line
                if re.match(r" ", line):
                    line = line.split()
                    values.append(line[0])
            if len(values) == 13:
                total_reads = int(values[0])*2
                unmap_reads = int(values[-3])
                map_reads = int(total_reads) - int(unmap_reads)
                map_reads = str(map_reads) + "(" + str(float("%0.4f" % (map_reads/int(total_reads))) * 100) + "%" + ")"
                uniq_mapped = int(values[2])*2 + int(values[6])*2 + int(values[-2])
                uniq_mapped = str(uniq_mapped) + "(" + str(float("%0.4f" % (uniq_mapped/int(total_reads))) * 100) + "%" + ")"
                multi_mapped = int(values[3])*2 + int(values[-1])
                multi_mapped = str(multi_mapped) + "(" + str(float("%0.4f" % (multi_mapped/int(total_reads))) * 100) + "%" + ")"
                print specimen_name
                print total_reads, map_reads, uniq_mapped, multi_mapped
                data = {
                    "project_sn": self.bind_object.sheet.project_sn,
                    "task_id": self.bind_object.sheet.id,
                    "type": "genome",
                    "specimen_name": specimen_name,
                    "total_reads": total_reads,
                    "mapping_reads": map_reads,
                    "multiple_mapped": multi_mapped,
                    "uniq_mapped": uniq_mapped
                }
                data_list.append(data)
            f.close()
        try:
            collection = self.db["sg_specimen_mapping"]
            collection.insert_many(data_list)
        except Exception, e:
            print("导入比对结果统计信息出错:%s" % e)
        else:
            print("导入比对结果统计信息成功")

if __name__ == "__main__":
    dir_path = "/mnt/ilustre/users/sanger-test/workspace/20170627/Refrna_mouse_6/MapAssessment/output/chr_stat"
