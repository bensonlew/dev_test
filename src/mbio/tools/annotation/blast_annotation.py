# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
# modified 2017.04.19
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.config import Config
import os
from biocluster.core.exceptions import OptionError
import subprocess
from mbio.packages.align.blast.blastout_statistics import *


class BlastAnnotationAgent(Agent):
    """
    对blast结果进行evalue, similarity, identity, length, score的筛选，重新对nr、Swissprot进行注释
    """
    def __init__(self, parent):
        super(BlastAnnotationAgent, self).__init__(parent)
        options = [
            {"name": "blastout_table", "type": "infile", "format": "align.blast.blast_table"},
            {"name": "evalue", "type": "float", "default": 10e-5},  # evalue值
            {"name": "score", "type": "float", "default": 0},  # score值
            {"name": "similarity", "type": "float", "default": 0},  # similarity值
            {"name": "identity", "type": "float", "default": 0},  # identity值
        ]
        self.add_option(options)
        self.step.add_steps("blast_anno")
        self.on("start", self.step_start)
        self.on("end", self.step_end)

    def step_start(self):
        self.step.blast_anno.start()
        self.step.update()

    def step_end(self):
        self.step.blast_anno.finish()
        self.step.update()

    def check_options(self):
        if not self.option("blastout_table").is_set:
            raise OptionError("必须提供BLAST的结果文件")
        if self.option("evalue"):
            if self.option("evalue") > 10e-3:
                raise OptionError("E-value需小于最大值10e-3")
        if self.option("similarity"):
            if self.option("similarity") > 1 or self.option("similarity") < 0:
                raise OptionError("similarity范围为0-1")
        if self.option("identity"):
            if self.option("identity") > 1 or self.option("identity") < 0:
                raise OptionError("identity范围为0-1")
        else:
            pass

    def set_resource(self):
        self._cpu = 10
        self._memory = "10G"

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            [r".*evalue\.xls", "xls", "比对结果E-value分布图"],
            [r".*similar\.xls", "xls", "比对结果相似度分布图"]
        ])
        super(BlastAnnotationAgent, self).end()


class BlastAnnotationTool(Tool):
    def __init__(self, config):
        super(BlastAnnotationTool, self).__init__(config)

    def run(self):
        super(BlastAnnotationTool, self).run()
        self.run_blast_filter()
        self.run_stat()
        self.end()

    def run_stat(self):
        self.logger.info("开始进行统计分析")
        try:
            blastout_statistics(blast_table=self.work_dir + "/blast_fliter.xls", evalue_path=self.output_dir + '/evalue.xls', similarity_path=self.output_dir + '/similar.xls')
            self.logger.info("统计分析完成")
        except Exception as e:
            self.set_error("运行统计出错:{}".format(e))

    def run_blast_filter(self):
        self.logger.info("开始进行blast参数筛选")
        evalue_list, score_list, similarity_list, identity_list = [], [], [], []
        with open(self.option("blastout_table").prop["path"], "rb") as f, open("blast_fliter.xls", "wb") as w:
            lines = f.readlines()
            w.write(lines[0])
            for i in range(1, len(lines)):
                line = lines[i].strip().split("\t")
                score = line[0]
                evalue = line[1]
                identity = line[3]
                similarity = line[4]
                if self.option("evalue"):
                    if self.option("evalue") >= float(evalue):
                        evalue_list.append(i)
                else:
                    evalue_list.sppend(i)
                if self.option("score"):
                    if self.option("score") <= float(score):
                        score_list.append(i)
                else:
                    score_list.append(i)
                if self.option("similarity"):
                    if self.option("similarity") * 100 <= float(similarity):
                        similarity_list.append(i)
                else:
                    similarity_list.append(i)
                if self.option("identity"):
                    if self.option("identity") * 100 <= float(identity):
                        identity_list.append(i)
                else:
                    identity_list.append(i)
            for j in range(1, len(lines)):
                if j in evalue_list:
                    if j in score_list:
                        if j in similarity_list:
                            if j in identity_list:
                                w.write(lines[j])
        self.logger.info("blast筛选完成")
