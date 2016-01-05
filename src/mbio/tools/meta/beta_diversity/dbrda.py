# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
# import os
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
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"}
        ]
        self.add_option(options)
        self.step.add_steps('dbRDA')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.dbRDA.start()
        self.step.update()

    def step_end(self):
        self.step.dbRDA.finish()
        self.step.update()

    def check_options(self):
        """
        检查参数
        :return: True
        """
        samplelist = []
        if not self.option('dis_matrix').is_set:
            raise OptionError('必须提供距离矩阵文件')
        else:
            self.option('dis_matrix').get_info()
            samplelist = self.option('dis_matrix').prop['samp_list']
        if not self.option('group').is_set:
            raise OptionError('必须提供分组信息文件')
        else:
            self.option('group').get_info()
            if len(samplelist) != len(self.option('group').prop['sample']):
                raise OptionError('分组文件中样本数量与距离矩阵中的样本数量不一致')
            for sample in self.option('group').prop['sample']:
                if sample not in samplelist:
                    raise OptionError('分组文件的样本(%s)在otu表的样本中不存在' % sample)
        return True

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
        # 模块脚本路径，并不使用

    def run(self):
        """
        运行
        """
        super(DbrdaTool, self).run()
        self.run_dbrda()

    def add_name(self):
        """
        给一个分组文件添加表头
        """
        groupfile = open(self.option('group').prop['path'], 'r')
        new = open(os.path.join(self.work_dir, 'temp.gup'), 'w')
        new.write('#ID\tgroup\n')
        for i in groupfile:
            new.write(i)
        groupfile.close()
        new.close()

    def linkfile(self, oldfile, newname):
        """
        link文件到output文件夹
        :param oldfile: 资源文件路径
        :param newname: 新的文件名
        :return:
        """
        newpath = os.path.join(self.output_dir, newname)
        if os.path.exists(newpath):
            os.remove(newpath)
        os.link(oldfile, newpath)

    def run_dbrda(self):
        """
        运行dbrda.py
        """
        self.logger.info('运行dbrda_r.py程序计算Dbrda')
        self.add_name()
        return_mess = db_rda(self.option('dis_matrix').prop['path'], self.work_dir + '/temp.gup', self.work_dir)
        if return_mess == 0:
            self.linkfile(self.work_dir + '/db_rda_factor.temp.txt', 'db_rda_factor.txt')
            self.linkfile(self.work_dir + '/db_rda_results.txt', 'db_rda_results.txt')
            self.linkfile(self.work_dir + '/db_rda_sites.temp.txt', 'db_rda_sites.txt')
            self.logger.info('运行dbrda_r.py程序计算Dbrda完成')
            self.end()
        else:
            self.set_error('运行dbrda_r.py程序计算Dbrda出错')
        
