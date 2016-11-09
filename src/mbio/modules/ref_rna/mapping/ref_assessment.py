#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import glob
from biocluster.core.exceptions import OptionError
from biocluster.module import Module


class RefAssessmentModule(Module):
    """
    denovoRNA比对后质量评估:基因覆盖率、比对结果统计、冗余序列分析
    version 1.0
    author: qindanhua
    last_modify: 2016.07.27
    """
    def __init__(self, work_id):
        super(RefAssessmentModule, self).__init__(work_id)
        options = [
            {"name": "bed", "type": "infile", "format": "denovo_rna.gene_structure.bed"},  # bed格式文件
            {"name": "bam", "type": "infile", "format": "ref_rna.assembly.bam_dir"},  # bam格式文件,排序过的
            {"name": "method", "type": "string", "default": "all"},
            {"name": "quality", "type": "int", "default": 30}  # 质量值    
        ]
        self.add_option(options)
        self.tools = []
        self.files = []
#        self.bam_stat = self.add_tool('denovo_rna.qc.fastq_stat')
        self.step.add_steps('stat')

    def finish_update(self, event):
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    def stat_finish_update(self):
        self.step.stat.finish()
        self.step.update()

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
                'bam': f
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
    
    def distribute_run(self):
        n = 0
        for f in self.files:
            distribute = self.add_tool("ref_rna.mapping.reads_distribution")
            self.step.add_steps("distribute_{}".format(n))
            distribute.set_options({
                "bam": f,
                "bed": self.option("bed").prop["path"]
            })
            step = getattr(self.step, "distribute_{}".format(n))
            step.start()
            distribute.on("end", self.finish_update, "distribute_{}".format(n))
            distribute.run()
            self.tools.append(distribute)
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
        dirs = ["coverage", "dup", "satur", "distribute"]
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
        bam_out = []
        for tool in self.tools:
            out = os.listdir(tool.output_dir)
            for f_name in out:
                fp = os.path.join(tool.output_dir, f_name)
                if f_name == "bam_stat.xls":
                    bam_out.append(fp)
                elif "DupRate" in f_name:
                    target = os.path.join(self.output_dir, "dup", f_name)
                    if os.path.exists(target):
                        os.remove(target)
                    os.link(fp, target)
                elif "satur_" in f_name:
                    target = os.path.join(self.output_dir, "satur", f_name)
                    if os.path.exists(target):
                        os.remove(target)
                    os.link(fp, target)            
                elif "geneBodyCoverage" in f_name:
                    target = os.path.join(self.output_dir, "coverage", f_name)
                    if os.path.exists(target):
                        os.remove(target)
                    os.link(fp, target)
                elif "reads_distribution" in f_name:
                    target = os.path.join(self.output_dir, "distribute", f_name)
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
        self.bam_stat_run()
        self.dup_run()
        self.satur_run()
        self.coverage_run()
        self.distribute_run()
        super(RefAssessmentModule, self).run()
        # self.on_rely(self.tools, self.set_output)
        

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        """
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["./coverage/", "", "基因覆盖度分析输出目录"],
            ["./dup/", "", "冗余序列分析输出目录"],
            ["./satur/", "", "测序饱和度分析输出目录"],
            ["./bam_stat.xls", "xls", "bam格式比对结果统计表"],
            ["./", "txt", "reads区域分布目录"]
        ])
        result_dir.add_regexp_rules([
            [r".*pos\.DupRate\.xls", "xls", "比对到基因组的序列的冗余统计表"],
            [r".*seq\.DupRate\.xls", "xls", "所有序列的冗余统计表"],
            [r".*eRPKM\.xls", "xls", "RPKM表"],
            [r".*cluster_percent\.xls", "xls", "饱和度作图数据"],
            [r".*distribution\.txt", "txt", "reads区域分布"]
        ])
        """
        super(RefAssessmentModule, self).end()
