#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from mbio.packages.denovo_rna.qc.fastq_stat import get_fastq_info
import os


class FastqBaseStatAgent(Agent):
    """
    用于做fastq序列基本信息统计的工具
    version 1.0
    author: qindanhua
    last_modify: 2016.06.22
    """

    def __init__(self, parent):
        super(FastqBaseStatAgent, self).__init__(parent)
        options = [
            {"name": "fastq_r", "type": "infile", "format": "sequence.fastq"},  # 输入文件PE的右端序列
            {"name": "fastq_l", "type": "infile", "format": "sequence.fastq"},  # PE的左端序列
            {"name": "fastq_s", "type": "infile", "format": "sequence.fastq"},  # 输入文件fastq序列
            # {"name": "fq_list", "type": "infile", "format": "sequence.file_sample"},
            {"name": "fq_type", "type": "string"}
        ]
        self.add_option(options)
        self.step.add_steps('fastq_base_stat')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.fastq_base_stat.start()
        self.step.update()

    def step_end(self):
        self.step.fastq_base_stat.finish()
        self.step.update()

    def check_options(self):
        """
        检测参数是否正确
        """
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError("请说明序列类型，PE or SE?")
        if self.option('fq_type') is ["PE"]:
            if not self.option('fastq_r') and self.option('fastq_l').is_set:
                raise OptionError("请选择输入文件")
        elif self.option('fq_type') is ["SE"]:
            if not self.option("fastq_s").is_set:
                raise OptionError("请选择输入文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["./fastq_stat.xls", "xls", "fastq信息统计表"]
        ])


class FastqBaseStatTool(Tool):
    """
    version 1.0
    """
    def __init__(self, config):
        super(FastqBaseStatTool, self).__init__(config)
        # self.FastqStat_path = "rna/scripts/FastqStat.jar"

    def get_fastq_info(self):
        self.logger.info("where")
        if self.option("fq_type") in ["SE"]:
            self.logger.info("stat")
            sample_name = self.option("fastq_s").prop["path"].split("/")[-1].split("_")[0]
            fastq_info = get_fastq_info(self.option("fastq_s").prop["path"])
            self.write_fastq_info(fastq_info, sample_name)
        elif self.option("fq_type") in ["PE"]:
            sample_name = self.option("fastq_s").prop["path"].split("/")[-1].split("_")[0]
            fastq_l = self.option("fastq_l").prop["path"]
            fastq_r = self.option("fastq_r").prop["path"]
            with open(fastq_l, "r") as l, open(fastq_r, "r") as r, open("%s.fq" % sample_name, "w") as w:
                for line in l:
                    w.write(line)
                for line in r:
                    w.write(line)
            fastq_info = get_fastq_info("%s.fq" % sample_name)
            self.write_fastq_info(fastq_info, sample_name)

    def write_fastq_info(self, fastq_info, sample_name):
        with open("fastq_stat.xls", "w") as w:
            w.write("#Sample_ID\tTotal_Reads\tTotal_Bases\tTotal_Reads_with_Ns\tN_Reads%\tA%\tT%\tC%\tG%\tN%\tError%\t"
                    "Q20%\tQ30%\tGC%\n")
            w.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t".format(sample_name, fastq_info["total_reads"], fastq_info["total_bases"], fastq_info["Total_Reads_with_Ns"], fastq_info["N_Reads%"], fastq_info["A%"], fastq_info["T%"], fastq_info["C%"], fastq_info["G%"], fastq_info["N%"], fastq_info["Error%"], fastq_info["Q20%"], fastq_info["Q30%"], fastq_info["CG%"]))

    def set_output(self):
        self.logger.info("set output")
        os.system('rm -rf '+self.output_dir)
        os.system('mkdir '+self.output_dir)
        os.link(self.work_dir+'/fastq_stat.xls', self.output_dir+'/fastq_stat.xls')
        self.logger.info("done")
        self.end()

    def run(self):
        """
        运行
        """
        super(FastqBaseStatTool, self).run()
        self.get_fastq_info()
        self.set_output()
