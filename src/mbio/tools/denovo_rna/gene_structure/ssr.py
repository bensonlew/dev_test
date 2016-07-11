# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import subprocess


class SsrAgent(Agent):
    """
    misa:SSR分析软件
    primer3：引物设计软件
    version 1.0
    author: qindanhua
    last_modify: 2016.07.11
    """

    def __init__(self, parent):
        super(SsrAgent, self).__init__(parent)
        options = [
            {"name": "fasta", "type": "infile", "format": "sequence.fasta"},  # 输入文件
            {"name": "primer", "type": "bool", "default": True},  # 是否设计SSR引物
            {"name": "bed", "type": "infile", "format": "denovo_rna.gene_structure.bed"}  # bed格式文件
        ]
        self.add_option(options)

    def check_options(self):
        """
        检查参数
        """
        if not self.option("fasta").is_set:
            raise OptionError("请传入fasta序列文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''


class SsrTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(SsrTool, self).__init__(config)
        self.misa_path = "rna/misa/"
        self.primer3_path = "rna/primer3-2.3.6/"
        self.fasta_name = self.option("fasta").prop["path"].split("/")[-1]
        self.ssr_position_path = "rna/scripts/misa.annot.pl"

    def misa(self):
        cmd = "{}misa.pl {}".format(self.misa_path, self.option("fasta").prop["path"])
        print(cmd)
        self.logger.info("开始运行misa")
        command = self.add_command("misa", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("运行misa结束！")
        else:
            self.set_error("运行misa过程出错")

    def primer(self):
        cmd = "{}primer3_core < {} > {}".format(self.primer3_path, self.fasta_name + ".misa.p3in",
                                                self.fasta_name + ".misa.p3in")
        print(cmd)
        self.logger.info("开始运行primer")
        command = self.add_command("primer", cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("运行primer结束")
        else:
            self.set_error("运行primer出错")

    def primer_in(self):
        p3_in_cmd = "{}p3_in.pl {}".format(self.misa_path, self.fasta_name + ".misa")
        self.logger.info("转换misa结果为primer输入格式")
        try:
            subprocess.check_output(p3_in_cmd, shell=True)
            self.logger.info("OK")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("转换文件过程出错")
            return False

    def primer_out(self):
        p3_out_cmd = "{}p3_in.pl {}".format(self.misa_path, self.fasta_name + ".misa.p3out")
        self.logger.info("转换misa结果为primer输出格式")
        try:
            subprocess.check_output(p3_out_cmd, shell=True)
            self.logger.info("OK")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("转换文件过程出错")
            return False

    def ssr_position(self):
        ssr_position_cmd = "{} -i {} -orf {}".format(self.fasta_name + ".misa", self.option("bed").prop["path"])
        self.logger.info("判断ssr位置")
        try:
            subprocess.check_output(ssr_position_cmd, shell=True)
            self.logger.info("OK")
            return True
        except subprocess.CalledProcessError:
            self.logger.info("过程出错")
            return False

    def set_output(self):
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        os.link(self.work_dir+'/' + self.fasta_name + ".misa", self.output_dir+'/' + self.fasta_name + ".misa")

    def run(self):
        """
        运行
        """
        super(SsrTool, self).run()
        if self.option("bed").is_set:
            self.misa()
            self.primer_in()
            self.primer_out()
            self.ssr_position()
        else:
            self.misa()
            self.primer_in()
            self.primer_out()
