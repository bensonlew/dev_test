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
        self.tools = []
        # self.bam_stat = self.add_tool('denovo_rna.mapping.bam_stat')
        self.coverage = self.add_tool('denovo_rna.mapping.coverage')
        # self.qual_assess = self.add_tool('denovo_rna.mapping.quality_assessment')
        self.step.add_steps('coverage')

    def finish_update(self, event):
        # obj = event['bind_object']
        # self.logger.info(event)
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    def coverage_finish_update(self):
        self.step.coverage.finish()
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
        files = self.get_files()
        n = 0
        for f in files:
            bam_stat = self.add_tool('denovo_rna.mapping.bam_stat')
            self.step.add_steps('bamStat_{}'.format(n))
            bam_stat.set_options({
                'bam': f
                })
            step = getattr(self.step, 'bamStat_{}'.format(n))
            step.start()
            bam_stat.on("end", self.finish_update, 'bamStat_{}'.format(n))
            bam_stat.run()
            self.tools.append(bam_stat)

    def coverage_run(self):
        self.coverage.set_options({
            'bam': self.option('bam').prop["path"],
            "bed": self.option('bed').prop["path"]
            })
        self.step.coverage.start()
        self.coverage.on("end", self.coverage_finish_update)
        self.coverage.run()
        self.tools.append(self.coverage)

    def qual_assess_run(self):
        files = self.get_files()
        for f in files:
            n = 0
            qual_assess = self.add_tool('denovo_rna.mapping.quality_assessment')
            self.step.add_steps('QualAssess_{}'.format(n))
            qual_assess.set_options({
                'bam': f,
                "bed": self.option('bed').prop["path"]
                })
            step = getattr(self.step, 'QualAssess_{}'.format(n))
            step.start()
            qual_assess.on("end", self.finish_update, 'QualAssess_{}'.format(n))
            qual_assess.run()
            self.tools.append(qual_assess)

    def get_files(self):
        f_path = self.option("bam").prop["path"]
        files = []
        if self.option("bam").format == "align.bwa.bam":
            files.append(f_path)
        elif self.option("bam").format == "align.bwa.bam_dir":
            for f in glob.glob(r"{}/*.bam".format(f_path)):
                files.append(os.path.join(f_path, f))
        return files

    def set_output(self):
        self.logger.info("set output")
        dirs = ["coverage", "dup", "satur"]
        for f in os.listdir(self.output_dir):
            f_path = os.path.join(self.output_dir, f)
            os.remove(f_path)
        for d in dirs:
            path = os.path.join(self.output_dir, d)
            if os.path.exists(path):
                shutil.rmtree(path)
            os.makedirs(path)
        with open("bam_stat.xls", "w") as w:
            w.write("sample\tmappped_num\trate\n")
            for f in glob.glob(r"{}/BamStat*/output/*"):
                with open(f, "r") as r:
                    r.readline()
                    w.write(r.next())
        from_path = os.path.join(self.work_dir, "bam_stat.xls")
        target_path = os.path.join(self.output_dir, "bam_stat.xls")
        os.link(from_path, target_path)
        for f in glob.glob(r"{}/QualityAssessment*/output/*"):
            self.logger.info(f)
            f_name = f.split("/")[-1]
            if "DupRate" in f:
                target_path = os.path.join(self.output_dir, "dup", f_name)
            else:
                target_path = os.path.join(self.output_dir, "satur", f_name)
            os.link(f, target_path)
        # for f in os.listdir(self.coverage.output_dir):
        #     from_path = os.path.join(self.coverage.output_dir, f)
        #     target_path = os.path.join(self.output_dir, "bam_stat", f)
        #     os.link(from_path, target_path)
        self.end()

    def run(self):
        # super(MapAssessmentModule, self).run()
        self.bam_stat_run()
        # self.coverage_run()
        self.qual_assess_run()
        self.on_rely(self.tools, self.set_output)
        # self.on_rely([self.bam_stat, self.coverage, self.qual_assess], self.set_output)
        super(MapAssessmentModule, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"]
        ])
        super(MapAssessmentModule, self).end()
