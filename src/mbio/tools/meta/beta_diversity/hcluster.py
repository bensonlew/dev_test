# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import subprocess
from biocluster.core.exceptions import OptionError


class HclusterAgent(Agent):
    """
    脚本plot-hcluster_tree.pl
    version v1.0
    author: shenghe
    last_modified:2015.11.17
    """

    def __init__(self, parent):
        super(HclusterAgent, self).__init__(parent)
        options = [
            {"name": "dis_matrix", "type": "infile",
                "format": "meta.beta_diversity.distance_matrix"},
            {"name": "newicktree", "type": "outfile",
                "format": "meta.beta_diversity.newick_tree"},
            {"name": "linkage", "type": "string", "default": "average"}
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('dis_matrix').is_set:
            raise OptionError('必须提供输入距离矩阵表')
        else:
            self.option('dis_matrix').check()
        if self.option('linkage') not in ['average', 'single', 'complete']:
            raise OptionError('错误的层级聚类方式：%s' % self.option('linkage'))

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 1
        self._memory = ''


class HclusterTool(Tool):

    def __init__(self, config):
        super(HclusterTool, self).__init__(config)
        self._version = 'v2.1-20140214'  # plot-hcluster_tree.pl版本
        self.cmd_path = os.path.join(
            self.config.SOFTWARE_DIR, 'meta/scripts/beta_diversity/plot-hcluster_tree.pl')

    def run(self):
        """
        运行
        """
        self.logger.info('start runing')
        super(HclusterTool, self).run()
        self.run_hcluster()
        self.logger.info('runing over')

    def run_hcluster(self):
        """
        运行plot-hcluster_tree.pl
        """
        cmd = self.cmd_path
        cmd += ' -i %s -o %s -m %s' % (
            self.option('dis_matrix').prop['path'], self.work_dir, self.option('linkage'))
        self.logger.info('运行plot-hcluster_tree.pl程序计算Hcluster')
        self.logger.info(cmd)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('生成 hc.cmd.r 文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成 hc.cmd.r 文件失败')
            self.set_error('无法生成 hc.cmd.r 文件')
        try:
            subprocess.check_output(self.config.SOFTWARE_DIR +
                                    '/R-3.2.2/bin/R --restore --no-save < %s/hc.cmd.r' % self.work_dir, shell=True)
            self.logger.info('生成树文件成功')
        except subprocess.CalledProcessError:
            self.logger.info('生成树文件失败')
            self.set_error('无法生成树文件')
        filename = self.work_dir + '/hcluster_tree_' + \
            os.path.basename(self.option('dis_matrix').prop[
                'path']) + '_' + self.option('linkage') + '.tre'
        linkfile = self.output_dir + '/hcluster.tre'
        self.logger.info(filename)
        self.logger.info(linkfile)
        if os.path.exists(linkfile):
            os.remove(linkfile)
        os.link(filename, linkfile)
        self.option('newicktree').set_path(linkfile)
        self.logger.info(self.option('newicktree').prop)
        self.end()
