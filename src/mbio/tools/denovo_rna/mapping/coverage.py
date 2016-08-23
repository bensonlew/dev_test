# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import glob


class CoverageAgent(Agent):
    """
    Rseqc-2.3.6:RNA测序分析工具
    version 1.0
    author: qindanhua
    last_modify: 2016.07.13
    """

    def __init__(self, parent):
        super(CoverageAgent, self).__init__(parent)
        options = [
            {"name": "bed", "type": "infile", "format": "denovo_rna.gene_structure.bed"},  # bed格式文件
            {"name": "bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # bam格式文件,排序过的
            # {"name": "min_len", "type": "int", "default": 100}  # Minimum mRNA length (bp).
        ]
        self.add_option(options)
        self.step.add_steps('coverage')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.coverage.start()
        self.step.update()

    def step_end(self):
        self.step.coverage.finish()
        self.step.update()

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
        self._memory = '50G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        super(CoverageAgent, self).end()


class CoverageTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(CoverageTool, self).__init__(config)
        self.python_path = "program/Python/bin/"
        self.samtools_path = "bioinfo/align/samtools-1.3.1/"
        self.bam_name = os.path.basename(self.option("bam").prop["path"]).split(".")[0]

    def index(self):
        cmds = []
        if self.option("bam").format == "align.bwa.bam":
            cmd = "{}samtools index {}".format(self.samtools_path, self.option("bam").prop["path"])
            bam_name = self.option("bam").prop["path"].split("/")[-1]
            command = self.add_command("index_{}".format(bam_name.lower()), cmd)
            command.run()
            cmds.append(command)
        elif self.option("bam").format == "align.bwa.bam_dir":
            for f in glob.glob(self.option("bam").pro["path"]):
                bam_name = f.split("/")[-1]
                cmd = "{}samtools index {}".format(self.samtools_path, f)
                command = self.add_command("index_{}".format(bam_name.lower()), cmd)
                command.run()
                cmds.append(command)
        return cmds

    def coverage(self):
        coverage_cmd = "{}geneBody_coverage.py  -i {} -r {} -o {}".\
            format(self.python_path, self.option("bam").prop["path"], self.option("bed").prop["path"], "coverage_" + self.bam_name)
        print(coverage_cmd)
        self.logger.info("开始运行geneBody_coverage.py脚本")
        coverage_command = self.add_command("coverage", coverage_cmd)
        coverage_command.run()
        self.wait()
        if coverage_command.return_code == 0:
            self.logger.info("运行脚本结束！")
        else:
            self.set_error("运行脚本过程出错")

    def set_output(self):
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        file_path = glob.glob(r"*Coverage.txt")
        print(file_path)
        for f in file_path:
            output_dir = os.path.join(self.output_dir, f)
            os.link(os.path.join(self.work_dir, f), output_dir)
        self.end()

    def run(self):
        """
        运行
        """
        super(CoverageTool, self).run()
        cmds = self.index()
        self.wait()
        for cmd in cmds:
            if cmd.return_code == 0:
                self.logger.info("运行{}结束！".format(cmd.name))
            else:
                self.set_error("运行{}过程出错".format(cmd.name))
        self.coverage()
        self.set_output()
