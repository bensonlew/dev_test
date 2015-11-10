# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class HclusterAgent(Agent):
    """
    脚本plot-hcluster_tree.pl
    version v1.0
    author: shenghe
    last_modified:2015.11.6
    """

    def __init__(self, parent):
        super(HclusterAgent, self).__init__()
        options = [
            {"name": "input", "type": "infile", "format": "distance_matrix"},
            # 输入文件距离矩阵
            {"name": "output", "type": "outfile", "format": "hcluster_outdir"},
            # 输出一个树文件到文件夹
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        :return:
        """
        if not self.option('input').is_set:
            raise OptionError(u'必须提供输入距离矩阵表')
        if not self.option('output').is_set:
            raise OptionError(u'必须指定输出文件夹')

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 1
        self._memory = ''


class HclusterTool(Tool):

    def __init__(self, config):
        super(HclusterTool, self).__init__(config)
        self._version = 'v2.1-20140214'  # plot-hcluster_tree.pl脚本中指定的版本
        self.cmd_path = 'meta/plot-hcluster_tree.pl'  # 暂不确定

    def run(self):
        """
        运行
        :return:
        """
        super(HclusterTool, self).run()
        self.run_hcluster()

    def run_hcluster(self):
        """
        运行plot-hcluster_tree.pl
        :return:
        """
        cmd = self.cmd_path
        cmd += ' -i %s -o %s -m average' % (
            self.option('input'), self.option('output'))
        self.logger.info(u'运行plot-hcluster_tree.pl程序计算Hcluster')
        hcluster_command = self.add_command('hcluster', cmd)
        hcluster_command.run()
        self.wait()
        if hcluster_command.return_code == 0:
            self.logger.info(u'运行plot-hcluster_tree.pl程序计算Hcluster完成')
            self.end()
        else:
            self.set_error(u'运行plot-hcluster_tree.pl程序计算Hcluster出错')
