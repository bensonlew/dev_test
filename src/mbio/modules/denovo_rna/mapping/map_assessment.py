#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import glob
from biocluster.core.exceptions import OptionError
from biocluster.module import Module


class MapAssessmentModule(Module):
    """
    denovoRNA比对后质量评估
    version 1.0
    author: qindanhua
    last_modify: 2016.07.14
    """
    def __init__(self, work_id):
        super(MapAssessmentModule, self).__init__(work_id)
        options = [
            {"name": "bed", "type": "infile", "format": "denovo_rna.gene_structure.bed"},  # bed格式文件
            {"name": "bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # bam格式文件,排序过的
            {"name": "quality", "type": "int", "default": 30}  # 质量值
        ]
        self.add_option(options)
        self.bam_stat = self.add_tool('denovo_rna.mapping.bam_stat')
        self.coverage = self.add_tool('denovo_rna.mapping.coverage')
        self.qual_assess = self.add_tool('denovo_rna.mapping.quality_assessment')
        self.step.add_steps('bam_stat', 'coverage', 'qual_assess')

    def bam_stat_finish_update(self):
        self.step.bam_stat.finish()
        self.step.update()

    def coverage_finish_update(self):
        self.step.coverage.finish()
        self.step.update()

    def qual_assess_finish_update(self):
        self.step.qual_assess.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数
        """
        if not self.option("bed").is_set:
            raise OptionError("请传入bed文件")
        if not self.option("bam").is_set:
            raise OptionError("请传入bam文件")

    def bam_stat_run(self):
        self.bam_stat.set_options({
            'bam': self.option('bam').prop["path"]
            })
        self.step.bam_stat.start()
        self.bam_stat.on("end", self.bam_stat_finish_update)
        self.bam_stat.run()

    def coverage_run(self):
        self.coverage.set_options({
            'bam': self.option('bam').prop["path"],
            "bed": self.option('bed').prop["path"]
            })
        self.step.coverage.start()
        self.coverage.on("end", self.coverage_finish_update)
        self.coverage.run()

    def qual_assess_run(self):
        self.qual_assess.set_options({
            'bam': self.option('bam').prop["path"],
            "bed": self.option('bed').prop["path"]
            })
        self.step.qual_assess.start()
        self.qual_assess.on("end", self.qual_assess_finish_update)
        self.qual_assess.run()

    def set_output(self):
        self.logger.info("set output")
        dirs = ["bam_stat", "coverage", "dup", "satur"]
        for f in os.listdir(self.output_dir):
            f_path = os.path.join(self.output_dir, f)
            os.remove(f_path)
        for d in dirs:
            path = os.path.join(self.output_dir, d)
            if os.path.exists(path):
                shutil.rmtree(path)
            os.makedirs(path)
        for f in os.listdir(self.bam_stat.output_dir):
            from_path = os.path.join(self.bam_stat.output_dir, f)
            target_path = os.path.join(self.output_dir, "bam_stat", f)
            os.link(from_path, target_path)
        for f in os.listdir(self.qual_assess.output_dir):
            from_path = os.path.join(self.qual_assess.output_dir, f)
            if "DupRate" in f:
                target_path = os.path.join(self.output_dir, "dup", f)
            else:
                target_path = os.path.join(self.output_dir, "satur", f)
            os.link(from_path, target_path)
        # for f in os.listdir(self.coverage.output_dir):
        #     from_path = os.path.join(self.coverage.output_dir, f)
        #     target_path = os.path.join(self.output_dir, "bam_stat", f)
        #     os.link(from_path, target_path)
        self.end()

    def run(self):
        super(MapAssessmentModule, self).run()
        self.bam_stat_run()
        # self.coverage_run()
        self.qual_assess_run()
        self.on_rely([self.bam_stat, self.qual_assess], self.set_output)
        # self.on_rely([self.bam_stat, self.coverage, self.qual_assess], self.set_output)

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"]
        ])
        super(MapAssessmentModule, self).end()
