#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import glob
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.module import Module


class SnpModule(Module):
    """
    denovoRNA比对后质量评估
    version 1.0
    author: qindanhua
    last_modify: 2016.07.14
    """
    def __init__(self, work_id):
        super(SnpModule, self).__init__(work_id)
        options = [
            {"name": "bed", "type": "infile", "format": "denovo_rna.gene_structure.bed"},  # bed格式文件
            {"name": "in_bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # bam格式文件,排序过的
            {"name": "varscan_method", "type": "string", "default": "pileup2snp"},  # 选择varscan的pileup2snp方法
            {"name": "samtools_method", "type": "string", "default": "mplieup"},  # 选择samtools的mpileup方法
            {"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"},  # 参考序列
        ]
        self.add_option(options)
        # self.varscan = self.add_tool('denovo_rna.gene_structure.varscan')
        # self.step.add_steps('varscan')

    def check_options(self):
        """
        检查参数
        """
        if not self.option("ref_fasta").is_set:
            raise OptionError("请传入参考序列")
        if not self.option("in_bam").is_set:
            raise OptionError("bam文件")

    def samtools_finish_update(self, event):
        # obj = event['bind_object']
        # self.logger.info(event)
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    def samtools_run(self):
        sam_tools = []
        files = []
        if self.option("in_bam").format == "align.bwa.bam":
            files.append(self.option("in_bam").prop["path"])
        else:
            files = glob.glob(self.option("in_bam").prop["path"])
        n = 1
        for f_path in files:
            self.logger.info(f_path)
            samtools = self.add_tool('denovo_rna.gene_structure.samtools')
            self.step.add_steps('samtools_{}'.format(n))
            samtools.set_options({
                "ref_fasta": self.option('ref_fasta').prop["path"],
                "in_bam": f_path,
                "method": "mpileup"
            })
            step = getattr(self.step, 'samtools_{}'.format(n))
            step.start()
            samtools.on("end", self.samtools_finish_update, 'samtools_{}'.format(n))
            # samtools.run()
            sam_tools.append(samtools)
            n += 1
        if len(sam_tools) == 1:
            sam_tools[0].on("end", self.varscan_run)
            sam_tools[0].run()
        else:
            for tool in sam_tools:
                tool.run()
            self.on_rely(sam_tools, self.varscan_run)

    def varscan_run(self):
        varscan_tools = []
        files = glob.glob("{}/Samtools*/output/*.pileup".format(self.work_dir))
        self.logger.info(files)
        n = 1
        for f_path in files:
            self.logger.info(f_path)
            varscan = self.add_tool('denovo_rna.gene_structure.varscan')
            self.step.add_steps('varscan_{}'.format(n))
            varscan.set_options({
                "pileup": f_path,
                "method": self.option("varscan_method")
            })
            step = getattr(self.step, 'varscan_{}'.format(n))
            step.start()
            varscan.on("end", self.samtools_finish_update, 'varscan_{}'.format(n))
            # varscan.run()
            varscan_tools.append(varscan)
            n += 1
        if len(varscan_tools) == 1:
            varscan_tools[0].on("end", self.set_output)
            varscan_tools[0].run()
        else:
            for tool in varscan_tools:
                tool.run()
            self.on_rely(varscan_tools, self.set_output)

    def set_output(self):
        self.logger.info("set output")
        for f in os.listdir(self.output_dir):
            f_path = os.path.join(self.output_dir, f)
            if os.path.isdir(f_path):
                shutil.rmtree(f_path)
            else:
                os.remove(f_path)
        self.logger.info("done")
        self.end()

    def run(self):
        """
        运行
        :return:
        """
        super(SnpModule, self).run()
        self.samtools_run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"]
        ])
        super(SnpModule, self).end()
