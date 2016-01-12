# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
from mbio.packages.sequence.search_fastx_by_id import *


class GetFastaByIdAgent(Agent):
    """
    GetFastaById:通过传入序列的id返回对应fasta文件
    version 1.0
    author: qindanhua
    last_modify: 2016.01.06
    """

    def __init__(self, parent):
        super(GetFastaByIdAgent, self).__init__(parent)
        options = [
            {"name": "fasta", "type": "infile", "format": "sequence.fasta"},
            {"name": "id_file", "type": "infile", "format": "sequence.fastx_id"},
            {"name": "if_id_file", "type": "bool", "default": False},
            {"name": "id", "type": "string"}
        ]
        self.add_option(options)
        self.step.add_steps('search_by_id')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.search_by_id.start()
        self.step.update()

    def step_end(self):
        self.step.search_by_id.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数是否正确
        """
        if not self.option("fasta").is_set:
            raise OptionError("请传入OTU代表序列文件")

    def set_resource(self):
        """
        所需资源
        """
        self._cpu = 10
        self._memory = ''


class GetFastaByIdTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(GetFastaByIdTool, self).__init__(config)

    def search_by_id(self):
        """
        通过传入序列的id号，返回相应序列
        """
        self.logger.info("开始查找序列")
        if self.option('if_id_file') is False:
            search_fasta_by_id(self.option('fasta').prop['path'], self.option('id'))
        elif self.option('if_id_file') is True:
            search_fasta_by_idfile(self.option('fasta').prop['path'], self.option('id_file').prop['path'])
        self.logger.info("查找完毕")
        self.set_output()

    def set_output(self):
        """
        设置结果文件路径
        """
        self.logger.info("set out put")
        for f in os.listdir(self.output_dir):
            os.remove(os.path.join(self.output_dir, f))
        os.link(self.work_dir+'/searchID_result.fasta', self.output_dir+'/searchID_result.fasta')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(GetFastaByIdTool, self).run()
        self.search_by_id()
        self.end()
