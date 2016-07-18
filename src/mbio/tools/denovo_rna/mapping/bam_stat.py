# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from __future__ import division
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import glob
import re


class BamStatAgent(Agent):
    """
    Rseqc-2.3.6:RNA测序分析工具
    version 1.0
    author: qindanhua
    last_modify: 2016.07.12
    """

    def __init__(self, parent):
        super(BamStatAgent, self).__init__(parent)
        options = [
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

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''


class BamStatTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(BamStatTool, self).__init__(config)
        self.python_path = "Python/bin/"
        self.bam_path = self.option("bam").prop["path"]

    def bamstat(self, bam):
        stat_cmd = "{}bam_stat.py -q {}  -i {}".format(self.python_path, self.option("quality"), bam)
        bam_name = bam.split("/")[-1]
        print(stat_cmd)
        self.logger.info("开始运行bam_stat.py脚本")
        stat_command = self.add_command("{}_stat".format(bam_name.lower()), stat_cmd)
        stat_command.run()
        return stat_command

    def multi_stat(self):
        files = os.listdir(self.bam_path)
        cmds = []
        for f in files:
            f_path = os.path.join(self.bam_path, f)
            f_cmd = self.bamstat(f_path)
            cmds.append(f_cmd)
        return cmds

    def set_output(self):
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        self.write_stat_table()
        os.link(self.work_dir+"/"+"bam_stat.xls", self.output_dir+"/"+"bam_stat.xls")
        self.logger.info("done")
        self.end()

    def write_stat_table(self):
        file_path = glob.glob(r"*.bam_stat.o")
        print(file_path)
        with open("bam_stat.xls", "w") as w:
            w.write("{}\t{}\t{}\n".format("sample", "mappped_num", "rate"))
            for fl in file_path:
                with open(fl, "r") as f:
                    total = 0
                    unmapped = 0
                    sample_name = fl.split(".")[0]
                    print sample_name
                    for line in f:
                        if re.match(r"Total", line):
                            total = line.split()[2]
                        if re.match(r"Unmapped", line):
                            unmapped = line.split()[2]
                    mapped = int(total) - int(unmapped)
                    # print mapped
                    write_line = "{}\t{}\t{}\n".format(sample_name, mapped, mapped/int(total))
                    w.write(write_line)
                    # print write_line

    def run(self):
        """
        运行
        """
        super(BamStatTool, self).run()
        if self.option("bam").format == "align.bwa.bam_dir":
            cmds = self.multi_stat()
            self.wait()
            for cmd in cmds:
                if cmd.return_code == 0:
                    self.logger.info("运行{}脚本结束".format(cmd.name))
                else:
                    self.set_error("运行{}过程出错".format(cmd.name))
        elif self.option("bam").format == "align.bwa.bam":
            cmd = self.bamstat(self.bam_path)
            self.wait()
            if cmd.return_code == 0:
                self.logger.info("运行{}脚本结束".format(cmd.name))
            else:
                self.set_error("运行{}过程出错".format(cmd.name))
        self.set_output()
