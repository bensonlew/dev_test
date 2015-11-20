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
    last_modified:2015.11.17
    """
    def __init__(self, parent):
        super(DbrdaAgent, self).__init__(parent)
        options = [
            {"name": "dis_matrix", "type": "infile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "dbrda_dir", "type": "outfile", "format": "meta.beta_diversity.dbrda_outdir"}
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('dis_matrix').is_set:
            raise OptionError('必须提供距离矩阵表')
        if not self.option('group').is_set:
            raise OptionError('必须提供分组信息表')

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = ''


class DbrdaTool(Tool):
    def __init__(self, config):
        super(DbrdaTool, self).__init__(config)
        self._version = '1.0'
        self.cmd_path = 'mbio/packages/beta_diversity/dbrda_r.py'
        # 脚本路径，并不使用

    def run(self):
        """
        运行
        """
        super(DbrdaTool, self).run()
        self.run_dbrda()

    def run_dbrda(self):
        """
        运行dbrda.py
        """
        self.logger.info('运行dbrda_r.py程序计算Dbrda')
        return_mess = db_rda(self.option('dis_matrix').prop['path'], self.option('group').prop['path'], self.work_dir)
        if return_mess == 0:
            factor = self.output_dir + '/db_rda_factor.txt'
            result = self.output_dir + '/db_rda_results.txt'
            sites = self.output_dir + '/db_rda_sites.txt'
            if os.path.exists(factor):
                os.remove(factor)
            if os.path.exists(result):
                os.remove(result)
            if os.path.exists(sites):
                os.remove(sites)
            os.link(self.work_dir + '/db_rda_factor.txt', factor)
            os.link(self.work_dir + '/db_rda_results.txt', result)
            os.link(self.work_dir + '/db_rda_sites.txt', sites)
            self.option('dbrda_dir').set_path(self.output_dir)
            self.logger.info(self.option('dbrda_dir').prop)
            self.logger.info('运行dbrda_r.py程序计算Dbrda完成')
            self.end()
        else:
            self.set_error('运行dbrda_r.py程序计算Dbrda出错')
        
