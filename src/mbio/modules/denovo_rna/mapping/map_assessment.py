#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import glob
from biocluster.core.exceptions import OptionError
from biocluster.module import Module


class MapAssessmentModule(Module):
    """
    denovoRNA比对后质量评估:基因覆盖率、比对结果统计、冗余序列分析
    version 1.0
    author: qindanhua
    last_modify: 2016.07.27
    """
    def __init__(self, work_id):
        super(MapAssessmentModule, self).__init__(work_id)
        options = [
            {"name": "bed", "type": "infile", "format": "denovo_rna.gene_structure.bed"},  # bed格式文件
            {"name": "bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # bam格式文件,排序过的
            {"name": "fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # 基因表达量表
            {"name": "analysis", "type": "string", "default": "satur,dup,coverage,correlation"},  # 分析类型
            {"name": "quality_satur", "type": "int", "default": 30},  # 测序饱和度分析质量值
            {"name": "quality_dup", "type": "int", "default": 30},  # 冗余率分析质量值
            {"name": "low_bound", "type": "int", "default": 5},  # Sampling starts from this percentile
            {"name": "up_bound", "type": "int", "default": 100},  # Sampling ends at this percentile
            {"name": "step", "type": "int", "default": 5},  # Sampling frequency
            {"name": "rpkm_cutof", "type": "float", "default": 0.01},  # RPKM阈值
            {"name": "min_len", "type": "int", "default": 100}  # Minimum mRNA length (bp).
        ]
        self.add_option(options)
        self.tools = []
        self.files = []
        self.correlation = self.add_tool('denovo_rna.mapping.correlation')
        # self.bam_stat = self.add_tool('denovo_rna.mapping.bam_stat')
        self.step.add_steps('stat', 'correlation')
        self.analysis = ["satur", "dup", "coverage", "correlation"]

    def finish_update(self, event):
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    def stat_finish_update(self):
        self.step.stat.finish()
        self.step.update()

    def correlation_finish_update(self):
        self.step.correlation.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数
        """
        if not self.option("bed").is_set:
            raise OptionError("请传入bed文件")
        if not self.option("bam").is_set:
            raise OptionError("请传入bam文件")
        for analysis in self.option("analysis").split(","):
            if analysis not in self.analysis:
                raise OptionError("所选质量评估分析方法不在范围内")
        self.files = self.get_files()

    # def bam_stat_run(self):
    #     self.bam_stat.set_options({
    #             'bam': self.option("bam").prop["path"]
    #             })
    #     self.step.stat.start()
    #     self.bam_stat.on("end", self.stat_finish_update)
    #     self.bam_stat.run()
    #     self.tools.append(self.bam_stat)

    def correlation_run(self):
        self.correlation.set_options({
                'fpkm': self.option("fpkm").prop["path"]
                })
        self.step.stat.start()
        self.correlation.on("end", self.correlation_finish_update)
        self.correlation.run()
        self.tools.append(self.correlation)

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
                "bed": self.option('bed').prop["path"],
                "low_bound": self.option("low_bound"),
                "up_bound": self.option("low_bound"),
                "step": self.option("step"),
                "rpkm_cutof": self.option("rpkm_cutof"),
                "quality": self.option("quality_satur")
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
                "quality": self.option("quality_dup")
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
                "bed": self.option('bed').prop["path"],
                "min_len": self.option("min_len")
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
        # make dir
        dirs = ["coverage", "dup", "satur", "correlation"]
        for f in os.listdir(self.output_dir):
            f_path = os.path.join(self.output_dir, f)
            if os.path.exists(f_path):
                if os.path.isdir(f_path):
                    shutil.rmtree(f_path)
                else:
                    os.remove(f_path)
        for d in dirs:
            f_path = os.path.join(self.output_dir, d)
            os.makedirs(f_path)
        self.logger.info(os.path.join(self.output_dir, "bam_stat.xls"))
        # link output
        bam_out = []
        for tool in self.tools:
            out = os.listdir(tool.output_dir)
            for f_name in out:
                fp = os.path.join(tool.output_dir, f_name)
                if f_name == "bam_stat.xls":
                    bam_out.append(fp)
                    # target = os.path.join(self.output_dir, f_name)
                    # if os.path.exists(target):
                    #     os.remove(target)
                    # os.link(fp, target)
                if "DupRate" in f_name:
                    target = os.path.join(self.output_dir, "dup", f_name)
                    if os.path.exists(target):
                        os.remove(target)
                    os.link(fp, target)
                if "satur_" in f_name:
                    target = os.path.join(self.output_dir, "satur", f_name)
                    if os.path.exists(target):
                        os.remove(target)
                    os.link(fp, target)
                if "geneBodyCoverage" in f_name:
                    target = os.path.join(self.output_dir, "coverage", f_name)
                    if os.path.exists(target):
                        os.remove(target)
                    os.link(fp, target)
                if "correlation" in f_name:
                    target = os.path.join(self.output_dir, "correlation", f_name)
                    if os.path.exists(target):
                        os.remove(target)
                    os.link(fp, target)
        with open(os.path.join(self.output_dir, "bam_stat.xls"), "w") as w:
            w.write("sample\tmappped_num\trate\n")
            for f in bam_out:
                with open(f, "r") as r:
                    r.readline()
                    for line in r:
                        w.write(line)
        self.end()

    def run(self):
        super(MapAssessmentModule, self).run()
        self.bam_stat_run()
        analysiss = self.option("analysis").split(",")
        for m in analysiss:
            if m == "satur":
                self.satur_run()
            if m == "dup":
                self.dup_run()
            if m == "coverage":
                self.coverage_run()
            if m == "correlation" and self.option("fpkm").is_set:
                self.correlation_run()
        self.on_rely(self.tools, self.set_output)
        # super(MapAssessmentModule, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "dir", "结果输出目录"],
            ["./coverage/", "dir", "基因覆盖度分析输出目录"],
            ["./dup/", "dir", "冗余序列分析输出目录"],
            ["./satur/", "dir", "测序饱和度分析输出目录"],
            ["./bam_stat.xls", "xls", "bam格式比对结果统计表"]
        ])
        result_dir.add_regexp_rules([
            [r".*pos\.DupRate\.xls", "xls", "比对到基因组的序列的冗余统计表"],
            [r".*seq\.DupRate\.xls", "xls", "所有序列的冗余统计表"],
            [r".*eRPKM\.xls", "xls", "RPKM表"],
            [r".*cluster_percent\.xls", "xls", "饱和度作图数据"],
            [r".correlation_matrix*\.xls", "xls", "相关系数矩阵"],
            [r".hcluster_tree*\.xls", "xls", "样本间相关系数树文件"]
        ])
        # print self.get_upload_files()
        super(MapAssessmentModule, self).end()
