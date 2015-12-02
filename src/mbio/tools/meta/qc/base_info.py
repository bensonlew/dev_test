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
            {"name": "fastq_path", "type": "infile", "format": "sequence.fastq_dir"}]  # 输入fastq文件夹
        self.add_option(options)

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("fastq_path").is_set:
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
        work_path = os.path.join(self.work_dir, "output")
        self.option('fastq_path').get_full_info(work_path)
        cmd_list = list()
        i = 0
        for fastq in self.option('fastq_path').prop['unzip_fastqs']:
            i += 1
            file_name = os.path.join(self.work_dir, "output",
                                     os.path.basename(fastq) + ".fastxstat")
            cmd = self.fastx_stats_path + " -i " + fastq + " -o " + file_name
            command = self.add_command("fastx_quality_stats" + str(i), cmd)
            cmd_list.append(command)
        for mycmd in cmd_list:
            self.logger.info("开始运行fastx_quality_stats")
            mycmd.run()
        self.wait()
        for mycmd in cmd_list:
            if mycmd.return_code == 0:
                self.logger.info("运行fastx_quality_stats完成")
            else:
                self.set_error("运行fastx_quality_stats出错")

    def run(self):
        """
        运行
        """
        super(BaseInfoTool, self).run()
        self._run_fastx()
        self.end()
