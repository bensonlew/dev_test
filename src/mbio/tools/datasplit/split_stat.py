# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""在备份完成之后，统计一次拆分和二次拆分的结果，生成json"""
from __future__ import division
import os
import re
import json
from collections import defaultdict
from biocluster.tool import Tool
from biocluster.agent import Agent
from biocluster.core.exceptions import OptionError


class SplitStatAgent(Agent):
    """
    对数据拆分的结果进行统计并生成json文件
    """
    def __init__(self, parent=None):
        super(SplitStatAgent, self).__init__(parent)
        self._run_mode = "ssh1"
        options = [
            {'name': 'sample_info', 'type': "infile", 'format': 'datasplit.miseq_split'},  # 样本拆分信息表
            {'name': 'fastx_path', 'type': "string"},  # fastx模块产生的统计结果的路径
            {'name': "time", 'type': "infile", 'format': 'datasplit.backup_time'},  # backup时所使用的month和year
            {'name': 'stat_dir', 'type': "string"}  # 统计文件夹，在模块second_split中产生
        ]
        self.add_option(options)

    def check_option(self):
        """
        参数检测
        """
        if not self.option('sample_info').is_set:
            raise OptionError("参数sample_info不能为空")
        if not self.option("time").is_set:
            raise OptionError("参数time不能为空")
        if not self.option('fastx_path'):
            raise OptionError("参数fastx_path不能为空")
        if not self.option('stat_dir'):
            raise OptionError("参数stat_dir不能为空")
        return True

    def set_resource(self):
        """
        设置所需要的资源
        """
        self._cpu = 1
        self._memory = ''


class SplitStatTool(Tool):
    def __init__(self, config):
        super(SplitStatTool, self).__init__(config)
        self._version = 1.0
        self.backup_dir = "/mnt/ilustre/users/sanger/data_split_tmp/"
        self.option('sample_info').get_info()
        self.option('time').get_info()
        self.p_shrot_read = defaultdict(int)
        self.p_valid_read = defaultdict(int)
        self.json_str = ""
        year = self.option('time').prop['year']
        month = self.option('time').prop['month']
        name = "id_" + str(self.option('sample_info').prop["split_id"]) +\
               "_" + str(self.option('sample_info').prop["sequcing_sn"])
        program = self.option('sample_info').prop["program"]
        self.seq_id = os.path.join(self.backup_dir, program, str(year), str(month), name)

    def create_json(self):
        stat_json = dict()
        stat_json["split_id"] = self.option('sample_info').prop["split_id"]
        stat_json["sequcing_id"] = self.option('sample_info').prop["sequcing_id"]
        stat_json["sequcing_sn"] = self.option('sample_info').prop["sequcing_sn"]
        report_path = os.path.join(self.seq_id, "Report")
        stat_json["html_path"] = report_path
        p_sample = list()
        c_sample = list()
        for c_id in self.option('sample_info').prop['child_ids']:
            my_c = self.get_child_sample_info(c_id)
            c_sample.append(my_c)
        for p_id in self.option('sample_info').prop['parent_ids']:
            my_p = self.get_parent_sample_info(p_id)
            p_sample.append(my_p)
        stat_json["parent_sample"] = p_sample
        stat_json["child_sample"] = c_sample
        json_path = os.path.join(self.work_dir, 'output', "stat.json")
        self.json_str = json.dumps(stat_json)
        with open(json_path, 'w') as w:
            json.dump(stat_json, w, indent=4)

    def get_child_sample_info(self, c_id):
        """
        获取一个子样本的信息
        """
        c_info = dict()
        # c_info["mj_sn"] = self.option('sample_info').child_sample(c_id, "mj_sn")
        primer = self.option('sample_info').child_sample(c_id, "primer")
        c_info["sample_id"] = c_id
        child_stat_file = os.path.join(self.option('stat_dir'), "c_stat.stat")
        c_stat = dict()
        with open(child_stat_file, 'r') as r:
            for line in r:
                line = line.rstrip('\n')
                line = re.split('\t', line)
                if line[0] == c_id:
                    c_stat["valid_reads"] = int(line[1])
                    c_stat["short_reads"] = int(line[2])
                    c_stat["total_reads"] = c_stat["valid_reads"] + c_stat["short_reads"]
                    p_id = self.option('sample_info').find_parent_id(c_id)
                    self.p_shrot_read[p_id] += c_stat["short_reads"]
                    self.p_valid_read[p_id] += c_stat["valid_reads"]
        fastq1 = dict()
        library_type = self.option('sample_info').parent_sample(p_id, "library_type")
        if library_type is None:
            library_type = "undefine"
        child_path = os.path.join(self.seq_id, library_type, "child")
        fastq1["file_path"] = os.path.join(child_path, c_info["sample_id"] + "_" + primer + ".fastq.gz")
        fastq1['size'] = self._get_size(fastq1["file_path"])
        c_info["stat"] = c_stat
        c_info["fastq_info"] = {"fastq1": fastq1}
        return c_info

    def get_parent_sample_info(self, p_id):
        """
        获取一个父样本的拆分信息
        """
        p_info = dict()
        # p_info["mj_sn"] = self.option('sample_info').parent_sample(p_id, "mj_sn")
        p_info["sample_id"] = p_id
        p_stat = dict()
        if self.option('sample_info').parent_sample(p_id, "has_child"):
            p_stat = self.get_p_stat(p_id)
        else:
            pear_stat_file = os.path.join(self.option('stat_dir'), "pear.stat")
            with open(pear_stat_file, 'r') as r:
                for line in r:
                    line = line.rstrip('\n')
                    line = re.split('\t', line)
                    if line[0] == p_id:
                        total_reads = line[1]
            p_stat["total_reads"] = int(total_reads)
            p_stat["merge_rate"] = 0
            p_stat["short_reads"] = 0
            p_stat["no_index_reads"] = 0
            p_stat["chimera_reads"] = 0
            p_stat["primer_missmatch_reads"] = 0
            p_stat["primer_missmatch_reads"] = 0
            p_stat["valid_reads"] = p_stat["total_reads"]
        fastq1 = dict()
        fastq2 = dict()
        library_type = self.option('sample_info').parent_sample(p_id, "library_type")
        if library_type is None:
            library_type = "undefine"
        parent_path = os.path.join(self.seq_id, library_type, "parent")
        fastq1 = self.get_parent_fastq_info(parent_path, p_info["sample_id"], "r1")
        fastq2 = self.get_parent_fastq_info(parent_path, p_info["sample_id"], "r2")
        p_info['stat'] = p_stat
        p_info["fastq_info"] = {"fastq1": fastq1, "fastq2": fastq2}
        return p_info

    def get_parent_fastq_info(self, parent_path, sample_id, suffix):
        fastq = dict()
        fastq["file_path"] = os.path.join(parent_path, sample_id + "_" + suffix + ".fastq.gz")
        fastq["quilty_table"] = os.path.join(parent_path, "fastx",
                                             sample_id + "_" + suffix + ".fastq.fastxstat")
        fastq["boxplot_png"] = os.path.join(parent_path, "fastx",
                                            sample_id + "_" + suffix + ".fastq.fastxstat.box.png")
        fastq["nucl_png"] = os.path.join(parent_path, "fastx",
                                         sample_id + "_" + suffix + ".fastq.fastxstat.nucl.png")
        q20q30 = os.path.join(parent_path, "fastx",
                              sample_id + "_" + suffix + ".fastq.q20q30")
        with open(q20q30, 'r') as r:
            line = r.readline().rstrip('\n')
            line = re.split('\t', line)
            q20 = float(line[3])
            line = r.readline().rstrip('\n')
            line = re.split('\t', line)
            q30 = float(line[3])
        my_q20 = max(q20, q30)
        my_q30 = min(q20, q30)
        fastq['q20'] = my_q20
        fastq['q30'] = my_q30
        fastq['size'] = self._get_size(fastq["file_path"])
        return fastq

    def get_p_stat(self, p_id):
        p_stat = dict()
        pear_stat_file = os.path.join(self.option('stat_dir'), "pear.stat")
        parent_stat_file = os.path.join(self.option('stat_dir'), "p_stat.stat")
        with open(pear_stat_file, 'r') as r:
            for line in r:
                line = line.rstrip('\n')
                line = re.split('\t', line)
                if line[0] == p_id:
                    total_reads = line[1]
                    merage_rate = line[2]
                    break
        with open(parent_stat_file, 'r') as r:
            for line in r:
                line = line.rstrip('\n')
                line = re.split('\t', line)
                if line[0] == p_id:
                    no_index_reads = line[1]
                    chimera_reads = line[2]
                    primer_missmatch_reads = line[3]
                    break
        p_stat["total_reads"] = int(total_reads)
        p_stat["merge_rate"] = merage_rate
        p_stat["short_reads"] = self.p_shrot_read[p_id]
        p_stat["no_index_reads"] = int(no_index_reads)
        p_stat["chimera_reads"] = int(chimera_reads)
        p_stat["primer_missmatch_reads"] = int(primer_missmatch_reads)
        p_stat["valid_reads"] = self.p_valid_read[p_id]
        return p_stat

    def _get_size(self, file_path):
        """
        统计一个文件的大小
        """
        size = int(os.stat(file_path).st_size)
        """
        level = {
            1: "B", 2: "KB", 3: "MB", 4: "GB", 5: "TB", 6: "PB"
        }
        i = 1
        while(i < 6 and size > 1000):
            size = size / 1000
            i += 1
        read_size = "{:.2f} {}".format(size, level[i])
        """
        return str(size)

    def run(self):
        super(SplitStatTool, self).run()
        self.create_json()
        self.end()
