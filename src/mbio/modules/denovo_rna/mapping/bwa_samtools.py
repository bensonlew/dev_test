#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import glob
from biocluster.core.exceptions import OptionError
from biocluster.module import Module


class BwaSamtoolsModule(Module):
    """
    denovoRNA比对后质量评估
    version 1.0
    author: qindanhua
    last_modify: 2016.07.13
    """
    def __init__(self, work_id):
        super(BwaSamtoolsModule, self).__init__(work_id)
        options = [
            {"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"},  # 参考序列
            {"name": "fq_type", "type": "string", "default": ""},  # fq类型，必传
            {"name": "fastq_r", "type": "infile", "format": "sequence.fastq"},  # 右端序列文件
            {"name": "fastq_l", "type": "infile", "format": "sequence.fastq"},  # 左端序列文件
            {"name": "fastq_s", "type": "infile", "format": "sequence.fastq"},  # SE序列文件
            {"name": "fastq_dir", "type": "infile", "format": "sequence.fastq_dir"},  # fastq文件夹
            {"name": "head", "type": "string", "default": None},  # 设置结果头文件
            # {"name": "sam", "type": "outfile", "format": "align.samtools.sam,align.samtools.sam_dir"},
            # {"name": "sam", "type": "infile", "format": "align.bwa.sam"},    # sam格式文件
            {"name": "out_bam", "type": "outfile", "format": "align.bwa.bam"},  # bam格式输入文件
            {"name": "method", "type": "string", "default": ""}     # samtool工具
        ]
        self.add_option(options)
        self.bwa = self.add_tool('align.bwa.bwa')
        # self.samtools = self.add_tool('denovo_rna.gene_structure.samtools')
        self.step.add_steps('bwa')

    def check_options(self):
        """
        检查参数
        """
        if not self.option("ref_fasta").is_set:
            raise OptionError("请传入参考序列")
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError("请说明序列类型，PE or SE?")
        if not self.option("fastq_dir").is_set and self.option('fq_type') in ["PE"]:
            if not self.option("fastq_r").is_set:
                raise OptionError("请传入PE右端序列文件")
            if not self.option("fastq_l").is_set:
                raise OptionError("请传入PE左端序列文件")
        if self.option('fq_type') in ["SE"] and not self.option("fastq_s").is_set:
            raise OptionError("请传入SE序列文件")
        return True

    def bwa_finish_update(self):
        self.step.bwa.finish()
        self.step.update()

    def samtools_finish_update(self, event):
        # obj = event['bind_object']
        # self.logger.info(event)
        step = getattr(self.step, event['data'])
        step.finish()
        self.step.update()

    def bwa_run(self):
        if self.option("fastq_dir").is_set:
            self.bwa.set_options({
                "ref_fasta": self.option('ref_fasta').prop["path"],
                'fastq_dir': self.option('fastq_dir').prop["path"],
                'fq_type': self.option('fq_type')
                })
        elif not self.option("fastq_dir").is_set:
            if self.option("fq_type") == "PE":
                self.bwa.set_options({
                    "ref_fasta": self.option('ref_fasta').prop["path"],
                    'fastq_l': self.option('fastq_l').prop["path"],
                    'fastq_r': self.option('fastq_r').prop["path"],
                    'fq_type': self.option('fq_type')
                    })
            elif self.option("fq_type") == "SE":
                self.bwa.set_options({
                    "ref_fasta": self.option('ref_fasta').prop["path"],
                    'fastq_s': self.option('fastq_s').prop["path"],
                    'fq_type': self.option('fq_type')
                    })
        # else:
        #     raise OptionError("文件类型不正确")
        self.step.bwa.start()
        self.bwa.on("end", self.bwa_finish_update)
        self.bwa.on("end", self.multi_samtools_run)
        self.bwa.run()

    def multi_samtools_run(self):
        tools = []
        files = os.listdir(self.bwa.output_dir)
        n = 1
        for f in files:
            f_path = os.path.join(self.bwa.output_dir, f)
            self.logger.info(f_path)
            samtools = self.add_tool('denovo_rna.gene_structure.samtools')
            self.step.add_steps('samtools_{}'.format(n))
            samtools.set_options({
                "ref_fasta": self.option('ref_fasta').prop["path"],
                "sam": f_path,
                "method": "sort"
            })
            step = getattr(self.step, 'samtools_{}'.format(n))
            step.start()
            samtools.on("end", self.samtools_finish_update, 'samtools_{}'.format(n))
            # samtools.run()
            tools.append(samtools)
            n += 1
        self.logger.info(len(tools))
        if len(tools) == 1:
            tools[0].on("end", self.set_output)
            tools[0].run()
        else:
            for tool in tools:
                tool.run()
            self.on_rely(tools, self.set_output)

    def set_output(self):
        self.logger.info("set output")
        for f in os.listdir(self.output_dir):
            f_path = os.path.join(self.output_dir, f)
            if os.path.isdir(f_path):
                shutil.rmtree(f_path)
            else:
                os.remove(f_path)
        sam_dir = os.path.join(self.output_dir, "sam")
        # self.option('sam').set_path(sam_dir)
        bam_dir = os.path.join(self.output_dir, "sorted_bam")
        # self.option('out_bam').set_path(bam_dir)
        os.makedirs(sam_dir)
        os.makedirs(bam_dir)
        for f in os.listdir(self.bwa.output_dir):
            bwa_output = os.path.join(self.bwa.output_dir, f)
            sam_output = os.path.join(sam_dir, f)
            os.link(bwa_output, sam_output)
        self.logger.info("{}/Samtools*".format(self.work_dir))
        samtools_out = glob.glob(r"{}/Samtools*".format(self.work_dir))
        for sam_out in samtools_out:
            sorted_bam = glob.glob(r"{}/*sorted.bam".format(sam_out))
            for bam in sorted_bam:
                target = bam_dir + "/" + bam.split("/")[-1]
                if os.path.exists(target):
                    os.remove(target)
                os.link(bam, target)
        self.logger.info("set output done")
        self.end()

    def run(self):
        super(BwaSamtoolsModule, self).run()
        self.bwa_run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r".", "", "结果输出目录"]
        ])
        super(BwaSamtoolsModule, self).end()
