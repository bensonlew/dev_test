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
    last_modify: 2016.07.27
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
        self.files = []
        # self.coverage = self.add_tool('denovo_rna.mapping.coverage')
        # self.step.add_steps('coverage')

    def finish_update(self, event):
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    # def coverage_finish_update(self):
    #     self.step.coverage.finish()
    #     self.step.update()

    def check_options(self):
        """
        检查参数
        """
        if not self.option("bed").is_set:
            raise OptionError("请传入bed文件")
        if not self.option("bam").is_set:
            raise OptionError("请传入bam文件")
        self.files = self.get_files()

    def bam_stat_run(self):
        n = 0
        for f in self.files:
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
            n += 1

    def satur_run(self):
        n = 0
        for f in self.files:
            satur = self.add_tool('denovo_rna.mapping.rpkm_saturation')
            self.step.add_steps('satur{}'.format(n))
            satur.set_options({
                'bam': f,
                "bed": self.option('bed').prop["path"]
                })
            step = getattr(self.step, 'satur{}'.format(n))
            step.start()
            satur.on("end", self.finish_update, 'satur{}'.format(n))
            satur.run()
            self.tools.append(satur)
            n += 1

    def dup_run(self):
        n = 0
        for f in self.files:
            dup = self.add_tool('denovo_rna.mapping.read_duplication')
            self.step.add_steps('dup_{}'.format(n))
            dup.set_options({
                'bam': f,
                "bed": self.option('bed').prop["path"]
                })
            step = getattr(self.step, 'dup_{}'.format(n))
            step.start()
            dup.on("end", self.finish_update, 'dup_{}'.format(n))
            dup.run()
            self.tools.append(dup)
            n += 1

    def coverage_run(self):
        n = 0
        for f in self.files:
            coverage = self.add_tool('denovo_rna.mapping.coverage')
            self.step.add_steps('coverage_{}'.format(n))
            coverage.set_options({
                'bam': f,
                "bed": self.option('bed').prop["path"]
                })
            step = getattr(self.step, 'coverage_{}'.format(n))
            step.start()
            coverage.on("end", self.finish_update, 'coverage_{}'.format(n))
            coverage.run()
            self.tools.append(coverage)
            n += 1

    def get_files(self):
        files = []
        if self.option("bam").format == "align.bwa.bam":
            files.append(self.option("bam").prop["path"])
        elif self.option("bam").format == "align.bwa.bam_dir":
            for f in glob.glob(r"{}/*.bam".format(self.option("bam").prop["path"])):
                files.append(os.path.join(self.option("bam").prop["path"], f))
        return files

    def set_output(self):
        self.logger.info("set output")
        dirs = ["coverage", "dup", "satur"]
        for d in dirs:
            path = os.path.join(self.output_dir, d)
            if os.path.exists(path):
                shutil.rmtree(path)
            os.makedirs(path)
        self.logger.info(os.path.join(self.output_dir, "bam_stat.xls"))
        bam_out = glob.glob(r"{}/BamStat*/output/*".format(self.work_dir))
        with open(os.path.join(self.output_dir, "bam_stat.xls"), "w") as w:
            w.write("sample\tmappped_num\trate\n")
            for f in bam_out:
                with open(f, "r") as r:
                    r.readline()
                    for line in r:
                        w.write(line)
        for f in glob.glob(r"{}/ReadDuplication*/output/*".format(self.work_dir)):
            f_name = os.path.basename(f)
            target_path = os.path.join(self.output_dir, "dup", f_name)
            os.link(f, target_path)
        for f in glob.glob(r"{}/RpkmSaturation*/output/*".format(self.work_dir)):
            f_name = os.path.basename(f)
            target_path = os.path.join(self.output_dir, "satur", f_name)
            os.link(f, target_path)
        for f in glob.glob(r"{}/Coverage*/output/*".format(self.work_dir)):
            f_name = os.path.basename(f)
            target_path = os.path.join(self.output_dir, "bam_stat", f_name)
            os.link(f, target_path)
        self.end()

    def run(self):
        # super(MapAssessmentModule, self).run()
        self.bam_stat_run()
        self.dup_run()
        self.satur_run()
        # self.coverage_run()
        self.on_rely(self.tools, self.set_output)
        super(MapAssessmentModule, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"],
            [r"./coverage/", "", "基因覆盖度分析输出目录"],
            [r"./dup/", "", "冗余序列分析输出目录"],
            [r"./satur/", "", "测序饱和度分析输出目录"],
            [r"./bam_stat.xls", "xls", "bam格式比对结果统计表"]
        ])
        super(MapAssessmentModule, self).end()
