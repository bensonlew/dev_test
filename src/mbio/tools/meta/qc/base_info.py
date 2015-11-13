# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import subprocess
from multiprocessing import Pool
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config


class BaseInfoAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.06
    """
    def __init__(self, parent):
        super(BaseInfoAgent, self).__init__(parent)
        options = [
            {"name": "fastq_path", "type": "infile", "format": "fastq_dir"},  # 输入文件夹
            {"name": "sample_number", "type": "string"},  # 项目中包含的样本的数目，应当和输入文件夹中的fastq文件的数目一致，用于检查是否有样本遗漏
            {"name": "base_info_path", "type": "outfile", "format": "base_info_dir"}]  # 输出的base_info文件夹
        self.add_option(options)
        self.fastx_stats_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/fastx_quality_stats")

    def check_options(self):
        """
        参数检测
        :return:
        """
        if not self.option("fastq_path").is_set:
            raise OptionError("参数fastq_path不能为空")
        if not self.option("sample_number").is_set:
            raise OptionError("必须设置参数sample_number")
        self.option("fastq_path").set_file_number(self.option("sample_number"))
        self.option("fastq_path").check()
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 10
        self._memory = ''


class BaseInfoTool(Tool):
    def __init__(self, config):
        super(BaseInfoTool, self).__init__(config)
        self._version = 1.0

    def _run_fastx(self):
        """
        统计每一个fastq的碱基质量
        result_path: fastx 统计文件的存放路径
        """
        self.result_path = os.path.join(self.option("fastq_path").prop["path"], "fastx_stats")
        p = Pool(processes=8)
        for fastq in self.option("fastq_path").unzip_file:
            p.apply_async(self._fastx_quality_stats, args=(fastq,))
        p.close()
        p.join()

    def _fastx_quality_stats(self, fastq):
        """
        运行fastx_quality_stats
        :param fastq: fastq文件的路径
        """
        file_name = os.path.join(self.result_path, os.path.basename(fastq) + ".base_info")
        try:
            subprocess.check_call(self.fastx_stats_path + " - i " + fastq
                                  + " -Q 33 -o " + file_name)
        except subprocess.CalledProcessError:
            raise Exception('_fastx_quality_stats 运行出错！')

    def run(self):
        """
        运行
        """
        super(BaseInfoTool, self).run()
        self._run_fastx()
