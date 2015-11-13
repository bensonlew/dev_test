# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
from mbio.packages.beta_diversity.dbrda_r import *


class DbrdaAgent(Agent):
    """
    dbrda_r.py
    version v1.0
    author: shenghe
    last_modified:2015.11.10
    """
    def __init__(self, parent):
        super(DbrdaAgent, self).__init__(parent)
        options = [
            {"name": "input1", "type": "infile", "format": "distance_matrix"},
            # 输入文件距离矩阵
            {"name": "input2", "type": "infile", "format": "group_table"},
            # 输入文件，分组信息
            {"name": "output", "type": "outfile", "format": "Dbrda_outdir"}
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        :return:
        """
        if not self.option('input1').is_set:
            raise OptionError('必须提供距离矩阵表')
        if not self.option('input2').is_set:
            raise OptionError('必须提供分组信息表')
        if not self.option('output').is_set:
            raise OptionError('必须指定输出文件夹')

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 1  # 需要资源数暂时不清楚
        self._memory = ''  #


class DbrdaTool(Tool):
    def __init__(self, config):
        super(DbrdaTool, self).__init__(config)
        self._version = '1.0'  # 脚本中指定的版本
        self.cmd_path = 'packages/dbrda_r.py'

    def run(self):
        """
        运行
        :return:
        """
        super(DbrdaTool, self).run()
        self.run_dbrda()

    def run_dbrda(self):
        """
        运行dbrda.py
        :return:
        """
        self.logger.info('运行dbrda_r.py程序计算Dbrda')
        return_mess = db_rda(self.option('input1'), self.option('input2'), self.option('output'))
        if return_mess == 0:
            self.logger.info('运行dbrda_r.py程序计算Dbrda完成')
            self.end()
        else:
            self.set_error('运行dbrda_r.py程序计算Dbrda出错')
        
