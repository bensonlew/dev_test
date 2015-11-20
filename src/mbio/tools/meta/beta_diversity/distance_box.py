# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
from mbio.files.meta.otu.group_table import GroupTableFile


class DistanceBoxAgent(Agent):
    """
    qiime 
    version 1.0
    author shenghe
    last_modified:2015.11.6
    """

    def __init__(self, parent):
        super(DistanceBoxAgent, self).__init__(parent)
        options = [
            {"name": "input1", "type": "infile", "format": "distance_matrix"},
            {"name": "output", "type": "outfile", "format": "dist_box_outdir"},
            {"name": "input2", "type": "infile", "format": "group_table"},
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        :return:
        """
        if not self.option('input1').is_set:
            raise OptionError('必须提供距离矩阵文件')
        if not self.option('input2').is_set:
            raise OptionError('必须提供分组信息文件')
        if not self.option('output').is_set:
            raise OptionError('必须提供输出文件夹')
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2  # 服务器，目前是一个样本的矩阵计算使用了6-8s
        self._memory = ''


class DistanceBoxTool(Tool):

    def __init__(self, config):
        super(DistanceBoxTool, self).__init__(config)
        self._version = '1.9.1'  # qiime版本
        self.cmd_path = 'python/lib/site-package/qiime/make_distance_boxplots.py'
        # 安装位置不确定，待定

    def run(self):
        """
        运行
        :return:
        """
        super(DistanceBoxTool, self).run()
        self.run_box()

    def run_box(self):
        """
        运行qiime/make_distance_boxplots.py
        :return:
        """
        cmd = self.cmd_path

        tempgroup = GroupTable()  # 实例化GroupTable
        tempgroup.set_path(self.option('input2'))
        tempgroup.get_info()
        groupname = tempgroup.prop['name']
        # 此文件实例目前没有完成，假定其有一个name的属性标示group名字

        cmd += ' -m %s -d %s -o %s -f %s' % (
            self.option('input2'), self.option('input1'),
            self.option('output'), groupname)
        self.logger.info('运行qiime/make_distance_boxplots.py程序')
        box_command = self.add_command('box', cmd)
        box_command.run()
        self.wait()
        if box_command.return_code == 0:
            self.logger.info('运行qiime/make_distance_boxplots.py完成')
            pass
            self.end()
        else:
            self.set_error('运行qiime/make_distance_boxplots.py出错')
