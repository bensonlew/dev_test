# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
# modified 2017.05.24
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.config import Config
import os
from biocluster.core.exceptions import OptionError
import subprocess
from mbio.packages.align.blast.blastout_statistics import *


class CazyAnnoAgent(Agent):
    """
    利用脚本对比对结果进行统计
    """
    def __init__(self, parent):
        super(CazyAnnoAgent, self).__init__(parent)
        options = [
            {"name": "hmmscan_result", "type": "infile", "format": "meta_genomic.hmmscan_table"},  # 比对结果文件
            {"name": "reads_profile_table", "type": "infile", "format": "sequence.profile_table"}  # gene_profile.reads_number.txt
        ]
        self.add_option(options)
        self.step.add_steps("cazy_anno")
        self.on("start", self.step_start)
        self.on("end", self.step_end)
        self._cpu = 1
        self._memory = ''

    def step_start(self):
        self.step.cazy_anno.start()
        self.step.update()

    def step_end(self):
        self.step.cazy_anno.finish()
        self.step.update()

    def check_options(self):
        if not self.option("hmmscan_result").is_set:
            raise OptionError("必须提供hmmscan比对结果作为输入文件")
        if not self.option("reads_profile_table").is_set:
            raise OptionError("必须提供非冗余基因集产生的gene_profile.reads_number.txt")
        return True

    def set_resource(self):
        self._cpu = 3
        self._memory = "5G"

    def end(self):
        # result_dir = self.add_upload_dir(self.output_dir)
        # result_dir.add_relpath_rules([
        #     [".", "", "结果输出目录"],
        # ])
        # result_dir.add_regexp_rules([
        #     [r".*evalue\.xls", "xls", "比对结果E-value分布图"],
        #     [r".*similar\.xls", "xls", "比对结果相似度分布图"]
        # ])
        super(CazyAnnoAgent, self).end()


class CazyAnnoTool(Tool):
    def __init__(self, config):
        super(CazyAnnoTool, self).__init__(config)
        self.python_path = "program/Python/bin/python"
        self.script_path = self.config.SOFTWARE_DIR + "/bioinfo/annotation/scripts/cazy_anno.py"
        self.class_def = self.config.SOFTWARE_DIR + "/database/CAZyDB/class_definition.txt"
        self.FamInfo = self.config.SOFTWARE_DIR + "/database/CAZyDB/FamInfo.txt"
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/program/Python/lib')
        self.set_environ(PATH=self.config.SOFTWARE_DIR + '/program/perl/perls/perl-5.24.0/bin')
        self.set_environ(PERLBREW_ROOT=self.config.SOFTWARE_DIR + '/program/perl')
        self.perl_script = "bioinfo/annotation/scripts/profile.sumGenesAbund.pl"

    def run(self):
        super(CazyAnnoTool, self).run()
        self.run_annot()
        self.set_output()
        self.end()

    def run_annot(self):
        cmd1 = '{} {} --out.dm {} --output_dir {} --class_def {} --FamInfo {}'.\
            format(self.python_path, self.script_path, self.option('hmmscan_result').prop['path'],
                   self.work_dir + "/gene.", self.class_def, self.FamInfo)
        self.logger.info("start cazy_anno")
        command = self.add_command("cazy_anno", cmd1).run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("cazy_anno done")
        else:
            self.set_error("cazy_anno error")
            raise Exception("cazy_anno error")
        cmd2 = '{} {},{} {} {},{}'.format(self.perl_script, self.work_dir + "/gene.cazy.family.stat.xls",
                                          self.work_dir + "/gene.cazy.class.stat.xls",
                                          self.option('reads_profile_table').prop['path'],
                                          self.output_dir + '/cazy.family.profile.xls',
                                          self.output_dir + '/cazy.class.profile.xls')
        self.logger.info("start cazy_profile")
        command2 = self.add_command("cazy_profile", cmd2).run()
        self.wait(command2)
        if command.return_code == 0:
            self.logger.info("cazy_profile done")
        else:
            self.set_error("cazy_profile error")
            raise Exception("cazy_profile error")

    def set_output(self):
        if len(os.listdir(self.output_dir)) == 2:
            self.logger.info("结果文件正确生成")
        else:
            self.logger.info("文件个数不正确，请检查")
            raise Exception("文件个数不正确，请检查")