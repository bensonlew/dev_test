# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import glob


class QualityAssessmentAgent(Agent):
    """
    Rseqc-2.3.6:RNA测序分析工具
    version 1.0
    author: qindanhua
    last_modify: 2016.07.13
    """

    def __init__(self, parent):
        super(QualityAssessmentAgent, self).__init__(parent)
        options = [
            {"name": "bed", "type": "infile", "format": "denovo_rna.gene_structure.bed"},  # bed格式文件
            {"name": "bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # bam格式文件,排序过的
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

    def rpkm_saturation(self, bam, out_pre):
        satur_cmd = "{}RPKM_saturation.py -i {} -r {} -o {} -q {}".format(self.python_path, bam, self.option("bed").prop["path"], out_pre, self.option("quality"))
        print(satur_cmd)
        bam_name = bam.split("/")[-1]
        self.logger.info("开始运行RPKM_saturation.py脚本")
        satur_command = self.add_command("{}_satur".format(bam_name.lower()), satur_cmd)
        satur_command.run()
        return satur_command

    def multi_satur(self, bam_dir, out_pre):
        cmds = []
        bams = glob.glob("{}/*".format(bam_dir))
        for bam in bams:
            cmd = self.rpkm_saturation(bam, out_pre)
            cmds.append(cmd)
        return cmds

    def duplication(self, bam, out_pre):
        dup_cmd = "{}read_duplication.py -i {} -o {} -q {}".format(self.python_path, bam, out_pre, self.option("quality"))
        print(dup_cmd)
        bam_name = bam.split("/")[-1]
        self.logger.info("开始运行read_duplication.py脚本")
        dup_command = self.add_command("{}_dup".format(bam_name), dup_cmd)
        dup_command.run()
        return dup_command

    def multi_dup(self, bam_dir, out_pre):
        cmds = []
        bams = glob.glob("{}/*".format(bam_dir))
        for bam in bams:
            cmd = self.duplication(bam, out_pre)
            cmds.append(cmd)
        return cmds

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
        saturation = self.rpkm_saturation(self.option("bam"), "satur")
        duplication = self.duplication(self.option("bam"), "dup")
        self.wait()
        if self.option("bam").format == "align.bwa.bam":
            if saturation.return_code == 0:
                self.logger.info("运行RPKM_saturation.py脚本结束！")
            else:
                self.set_error("运行RPKM_saturation.py脚本过程出错")
            if duplication.return_code == 0:
                self.logger.info("运行read_duplication.py脚本结束！")
            else:
                self.set_error("运行read_duplication.py脚本过程出错")
        else:
            for cmd in saturation:
                if cmd.return_code == 0:
                    self.logger.info("运行{}结束!".format(cmd.name))
                else:
                    self.set_error("运行{}结束!".format(cmd.name))
            for cmd in duplication:
                if cmd.return_code == 0:
                    self.logger.info("运行{}结束!".format(cmd.name))
                else:
                    self.set_error("运行{}结束!".format(cmd.name))
        self.set_output()

