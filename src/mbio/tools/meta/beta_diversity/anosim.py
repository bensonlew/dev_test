# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
from mbio.files.group_table import GroupTable
from mbio.files.anosim_outdir import AnosimOutdir


class AnosimAgent(Agent):
    """
    qiime
    version v1.0
    author: shenghe
    last_modified:2015.11.6
    """

    def __init__(self, parent):
        super(AnosimAgent, self).__init__(parent)
        options = [
            {"name": "input1", "type": "infile", "format": "distance_matrix"},
            {"name": "output", "type": "outfile", "format": "anosim_outdir"},
            {"name": "input2", "type": "infile", "format": "group_table"}
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        :return:
        """
        if not self.option('input1').is_set:
            raise OptionError(u'必须提供距离矩阵文件')
        if not self.option('input1').is_set:
            raise OptionError(u'必须提供分组信息文件')
        if not self.option('output').is_set:
            raise OptionError(u'必须提供输出文件夹')
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2  # 暂定
        self._memory = ''


class AnosimTool(Tool):

    def __init__(self, config):
        super(AnosimTool, self).__init__(config)
        self._version = '1.9.1'  # qiime版本
        self.cmd_path = 'python/lib/site-package/qiime/compare_categories.py'
        # 安装位置不确定，待定

    def run(self):
        """
        运行

        :return:
        """
        super(AnosimTool, self).run()
        self.run_compare_categories()

    def run_compare_categories(self,ddd):
        """
        ddddddd

        :param ddd:
        :return:
        """
        cmd = self.cmd_path

        tempgroup = GroupTable()  # 实例化GroupTable
        tempgroup.set_path(self.option('input2'))
        tempgroup.get_info()
        groupname = tempgroup.prop['name']
        # 此文件实例目前没有完成，假定其有一个name的属性标示group名字
        cmd1 = cmd + ' --method anosim -m %s -i %s -o %s -c %s' % (
            self.option('input2'), self.option('input1'),
            self.option('output'), groupname)
        cmd2 = cmd + ' --method adonis -m %s -i %s -o %s -c %s' % (
            self.option('input2'), self.option('input1'),
            self.option('output'), groupname)
        self.logger.info(u'运行qiime/compare_categories.py,计算adonis/anosim程序')
        dist_anosim_command = self.add_command('anosim', cmd1)
        dist_anosim_command.run()
        self.wait()
        if dist_anosim_command.return_code == 0:
            self.logger.info(u'运行qiime/compare_categories.py计算anosim完成')
        else:
            self.set_error(u'运行qiime/compare_categories.py计算anosim出错')
        dist_adonis_command = self.add_command('adonis', cmd2)
        dist_adonis_command.run()
        self.wait()
        if dist_adonis_command.return_code == 0:
            self.logger.info(u'运行qiime/compare_categories.py计算adonis完成')
            self.format_result()
            self.end()
        else:
            self.set_error(u'运行qiime/compare_categories.py计算adonis出错')

    def format_result(self):
        """
        整理anosim和adonis两个的结果到一个表中

        :return:
        """
        result = AnosimOutdir()  # 需要创建类型，在类型中设定合并两个文件的方法
        result.set_path(self.option('output').rstrip('/'))
        if result.prop['merge']:  # 应该设定merge属性
            pass
        else:
            result.merge()  # 写merge方法
