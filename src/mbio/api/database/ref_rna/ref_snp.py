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
import json
import glob
import re
import os


class RefSnp(Base):
    def __init__(self, bind_object):
        super(RefSnp, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'

    def add_snp_main(self, snp_anno=None):
        """
        导入SNP主表信息
        :param snp_anno: snp模块的结果文件夹，如果传就直接导入detail信息~/output_dir/
        :return:
        """
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": self.bind_object.sheet.id,
            "name": "snp_origin",
            "status": "end",
            "desc": "SNP主表",
            "snp_type": ["A/T", "A/C", "A/G", "C/G", "C/T", "G/C"],
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_snp"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        if snp_anno:
            self.add_snp_detail(snp_anno, inserted_id)
        # add_coverage_detail(coverage, inserted_id)
        return inserted_id

    def add_snp_detail(self, snp_anno, snp_id):
        """
        导入SNP详情表的函数
        :param snp_anno: snp模块的结果文件夹，即~/output_dir/
        :param snp_id: SNP主表的ID
        :return:
        """
        snp_files = glob.glob("{}/*".format(snp_anno))
        graph_data_list = []
        snp_types = []
        sample_names = []
        chroms = set()
        distributions = []
        print snp_files
        for sf in snp_files:
            data_list = []
            with open(sf, "r") as f:
                snp_type_stat = {}
                snp_pos_stat = {}
                indel_pos_stat = {}
                depth_stat = {"<30": 0, "30-100": 0, "100-200": 0, "200-300": 0, "300-400": 0, "400-500": 0, ">500": 0}
                f.readline()
                # print f.next().split("\t")
                sample_name = os.path.basename(sf).split(".")[0]
                sample_names.append(sample_name)
                print sample_name
                for line in f:
                    # print len(line)
                    line = line.strip().split("\t")
                    snp_type = line[3] + "/" + line[4]
                    data = {
                        "snp_id": snp_id,
                        "specimen_name": sample_name,
                        "type": "snp" if len(line[3]) + len(line[4]) == 2 and "-" not in snp_type else "indel",
                        "chrom": line[0],
                        "start": line[1],
                        "end": line[2],
                        "ref": line[3],
                        "alt": line[4],
                        "reads_num": int(line[5]),
                        "mut_rate": 0.33,
                        "anno": line[6],
                        "gene": line[7],
                        "mut_type": line[8],
                        "mut_info": line[9],
                        "snp_type": snp_type
                    }
                    chroms.add(line[0])
                    data_list.append(data)
                    if "-" not in snp_type and len(snp_type) < 4:
                        if snp_type in snp_type_stat:
                            snp_type_stat[snp_type] += 1
                        else:
                            snp_type_stat[snp_type] = 1
                    # 统计reads深度
                    depth_num = int(line[5])
                    if depth_num < 31:
                        depth_stat["<30"] += 1
                    elif 30 < depth_num < 101:
                        depth_stat["30-100"] += 1
                    elif 100 < depth_num < 201:
                        depth_stat["100-200"] += 1
                    elif 200 < depth_num < 301:
                        depth_stat["200-300"] += 1
                    elif 300 < depth_num < 401:
                        depth_stat["300-400"] += 1
                    elif 400 < depth_num < 501:
                        depth_stat["400-500"] += 1
                    else:
                        depth_stat[">500"] += 1
                    # 统计SNP位置信息/区域分布
                    if data["type"] == "snp":
                        if data["anno"] in snp_pos_stat:
                            snp_pos_stat[data["anno"]] += 1
                        else:
                            snp_pos_stat[data["anno"]] = 1
                    else:
                        if data["anno"] in indel_pos_stat:
                            indel_pos_stat[data["anno"]] += 1
                        else:
                            indel_pos_stat[data["anno"]] = 1
            import time
            time.sleep(20)
            graph_data = {
                "snp_id": snp_id,
                "specimen_name": sample_name,
                "snp_pos_stat": snp_pos_stat,
                "indel_pos_stat": indel_pos_stat,
                "type_stat": snp_type_stat,
                "depth_stat": depth_stat,
                "freq_stat": {}
            }
            snp_types = snp_type_stat.keys()
            print snp_type_stat
            try:
                collection = self.db["sg_snp_detail"]
                collection.insert_many(data_list)
            except Exception, e:
                print("导入snp结果统计信息出错:%s" % e)
            else:
                print("导入snp结果统计信息成功")
            graph_data_list.append(graph_data)
            # print depth_stat
            # print
            # print indel_pos_stat
        # print graph_data_list
            distributions = snp_pos_stat.keys()
        try:
            # collection = conn["sg_snp_detail"]
            # collection.insert_many(data_list)
            graph_collection = self.db["sg_snp_graphic"]
            graph_collection.insert_many(graph_data_list)
            main_collection = self.db["sg_snp"]
            main_collection.update({"_id": ObjectId(snp_id)}, {"$set": {"snp_types": snp_types, "specimen": sample_names, "distributions": distributions, "chroms": list(chroms)}})
        except Exception, e:
            print("导入SNP统计信息出错:%s" % e)
        else:
            print("导入SNP统计信息成功")