# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
import glob
import re
import os


class DenovoRnaSample(Base):
    def __init__(self, bind_object):
        super(DenovoRnaSample, self).__init__(bind_object)
        self._db_name = Config().MONGODB

    @report_check
    def add_samples_info(self, qc_stat, qc_adapt=None, fq_type='se'):
        stat_file = qc_stat + "/fastq_stat.xls"
        dup_file = qc_stat + "/dup.xls"
        dup = ""
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
            dup_rate = {}
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
                        "q30_rate": float(line[11]),
                        "q20_rate": float(line[12]),
                        "gc_rate": float(line[13]),
                        "about_qc": "before",
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
                collection = self.db["sg_denovo_specimen"]
                result = collection.insert_many(data_list)
            except Exception, e:
                self.bind_object.logger.error("导入样品信息数据出错:%s" % e)
            else:
                self.bind_object.logger.info("导入样品信息数据成功:%s" % result.inserted_ids)

    @report_check
    def add_gragh_info(self, quality_stat, about_qc="before"):
        stat_files = glob.glob("{}/*".format(quality_stat))
        data_list = []
        for sf in stat_files:
            sample_name = os.path.basename(sf).split("_")[0]
            sample_id = self.get_sample_id(sample_name, about_qc)
            site = os.path.basename(sf).split("_")[1]
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
                        "specimen_id": sample_id,
                        "type": site_type,
                        "about_qc": about_qc,
                        "column": int(line[0]),
                        "min": int(line[2]),
                        "max": int(line[3]),
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
            collection = self.db["sg_denovo_specimen_graphic"]
            result = collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入样品画图数据信息出错:%s" % e)
        else:
            self.bind_object.logger.info("导入样品画图数据信息成功:%s" % result.inserted_ids)

    @report_check
    def get_sample_id(self, sample_name, about_qc):
        collection = self.db["sg_denovo_specimen"]
        results = collection.find({"sg_denovo_specimen": sample_name, "about_qc": about_qc})
        sample_id = None
        if results.count():
            for result in results:
                sample_id = result["_id"]
        return sample_id
