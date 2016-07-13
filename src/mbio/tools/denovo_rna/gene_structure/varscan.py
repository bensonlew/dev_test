# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class VarscanAgent(Agent):
    """
    varscan:SNP calling软件
    version 1.0
    author: qindanhua
    last_modify: 2016.07.11
    """

    Method = ["pileup2snp", "mpileup2snp", "pileup2indel", "mpileup2indel"]

    def __init__(self, parent):
        super(VarscanAgent, self).__init__(parent)
        options = [
            {"name": "pileup", "type": "infile", "format": "denovo_rna.gene_structure.pileup"},  # mpileup 输出格式
            {"name": "method", "type": "string", "default": "pileup2snp"},  # mpileup 输出格式
            # {"name": "vcf", "type": "outfile", "format": "vcf"}     # Variant Call Format
        ]
        self.add_option(options)

    def check_options(self):
        """
        检查参数
        """
        if not self.option("pileup").is_set:
            raise OptionError("请传入pileup文件")
        if self.option("method") not in self.Method:
            raise OptionError("选择正确的工具")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''


class VarscanTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(VarscanTool, self).__init__(config)
        self.varscan_path = "/mnt/ilustre/users/sanger/app/rna/VarScan.v2.3.9.jar"
        self.java_path = "sun_jdk1.8.0/bin/"

    def pileup2snp(self):
        cmd = "{}java -jar {} pileup2snp {} --min-coverage 8 --min-reads2 3 --min-strands2 2 --min-avg-qual 30 " \
              "--min-var-freq 0.30".format(self.java_path, self.varscan_path, self.option("pileup").prop["path"])
        self.logger.info("开始运行pileup2snp")
        command = self.add_command("pileup2snp", cmd)
        self.logger.info(cmd)
        command.run()
        self.wait()
        if command.return_code == 0:
            self.logger.info("运行pileup2snp结束")
        else:
            self.set_error("运行pileup2snp出错")

    def set_output(self):
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        os.link(self.work_dir+'/pileup2snp.o', self.output_dir+'/pileup_out.xls')

    def run(self):
        """
        运行
        """
        super(VarscanTool, self).run()
        if self.option("method") == "pileup2snp":
            self.pileup2snp()
        self.set_output()
        self.end()
