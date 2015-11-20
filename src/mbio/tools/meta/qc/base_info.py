# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError


class BaseInfoAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.06
    """
    def __init__(self, parent):
        super(BaseInfoAgent, self).__init__(parent)
        options = [
            {"name": "fastq", "type": "infile", "format": "sequence.fastq"},  # 输入fastq文件
            {"name": "base_info", "type": "outfile", "format": "meta.qc.base_info"}]  # 输出的base_info文件
        self.add_option(options)

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("fastq").is_set:
            raise OptionError("参数fastq不能为空")
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 1
        self._memory = ''


class BaseInfoTool(Tool):
    def __init__(self, config):
        super(BaseInfoTool, self).__init__(config)
        self._version = 1.0
        self.fastx_stats_path = "fastxtoolkit/bin/fastx_quality_stats"

    def _run_fastx(self):
        file_name = os.path.join(self.work_dir, "output",
                                 os.path.basename(self.option("fastq").prop['path']) + ".fastxstat")
        cmd = self.fastx_stats_path + " -i " + self.option("fastq").prop['path']\
            + " -Q 33 -o " + file_name
        self.logger.info("开始运行fastx_quality_stats")
        command = self.add_command("fastx_quality_stats", cmd)
        command.run()
        self.wait(command)
        if command.return_code == 0:
            self.logger.info("运行fastx_quality_stats完成")
            self.option("base_info").set_path(file_name)
        else:
            self.set_error("运行fastx_quality_stats出错")

    def run(self):
        """
        运行
        """
        super(BaseInfoTool, self).run()
        self._run_fastx()
        self.end()
