# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
# import subprocess
import glob


class QualityAssessmentAgent(Agent):
    """
    Rseqc-2.3.6:RNA测序分析工具
    version 1.0
    author: qindanhua
    last_modify: 2016.07.08
    """

    def __init__(self, parent):
        super(QualityAssessmentAgent, self).__init__(parent)
        options = [
            {"name": "bed", "type": "infile", "format": "denovo_rna.gene_structure.bed"},  # bed格式文件
            {"name": "bam", "type": "infile", "format": "align.bwa.bam"},  # bam格式文件,排序过的
            {"name": "quality", "type": "int", "default": 30}  # 质量值
        ]
        self.add_option(options)

    def check_options(self):
        """
        检查参数
        """
        if not self.option("bam").is_set:
            raise OptionError("请传入比对结果bam格式文件")
        if not self.option("bed").is_set:
            raise OptionError("请传入bed格式文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''


class QualityAssessmentTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(QualityAssessmentTool, self).__init__(config)
        self.python_path = "Python/bin/"

    def rpkm_saturation(self):
        satur_cmd = "{}RPKM_saturation.py -i {} -r {} -o {}".format(self.python_path, self.option("bam").prop["path"],
                                                                    self.option("bed").prop["path"], "rpkm")
        print(satur_cmd)
        self.logger.info("开始运行RPKM_saturation.py脚本")
        satur_command = self.add_command("satur", satur_cmd)
        satur_command.run()
        return satur_command
        # if satur_command.return_code == 0:
        #     self.logger.info("运行脚本结束！")
        # else:
        #     self.set_error("运行脚本过程出错")

    def coverage(self):
        coverage_cmd = "{}geneBody_coverage.py  -i {} -r {} -o {}".\
            format(self.python_path, self.option("bam").prop["path"], self.option("bed").prop["path"], "coverage")
        print(coverage_cmd)
        self.logger.info("开始运行geneBody_coverage.py脚本")
        coverage_command = self.add_command("coverage", coverage_cmd)
        coverage_command.run()
        return coverage_command
        # self.wait()
        # if coverage_command.return_code == 0:
        #     self.logger.info("运行脚本结束！")
        # else:
        #     self.set_error("运行脚本过程出错")

    def duplication(self):
        dup_cmd = "{}read_duplication.py -i {} -o {}".format(self.python_path, self.option("bam").prop["path"], "dup")
        print(dup_cmd)
        self.logger.info("开始运行read_duplication.py脚本")
        dup_command = self.add_command("dup", dup_cmd)
        dup_command.run()
        return dup_command
        # self.wait()
        # if dup_command.return_code == 0:
        #     self.logger.info("运行脚本结束！")
        # else:
        #     self.set_error("运行脚本过程出错")

    def set_output(self):
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        file_path = glob.glob(r"rpkm*")
        print(file_path)
        for f in file_path:
            output_dir = os.path.join(self.output_dir, f)
            os.link(os.path.join(self.work_dir, f), output_dir)
        self.end()

    def run(self):
        """
        运行
        """
        super(QualityAssessmentTool, self).run()
        saturation = self.rpkm_saturation()
        coverage = self.coverage()
        duplication = self.duplication()
        self.wait()
        if saturation.return_code == 0:
            self.logger.info("运行RPKM_saturation.py脚本结束！")
        else:
            self.set_error("运行RPKM_saturation.py脚本过程出错")
        if coverage.return_code == 0:
            self.logger.info("运行geneBody_coverage.py脚本结束！")
        else:
            self.set_error("运行geneBody_coverage.py脚本过程出错")
        if duplication.return_code == 0:
            self.logger.info("运行read_duplication.py脚本结束！")
        else:
            self.set_error("运行read_duplication.py脚本过程出错")
        self.set_output()

