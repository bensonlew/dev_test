# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class DistanceBoxAgent(Agent):
    """
    qiime 
    version 1.0
    author shenghe
    last_modified:2015.11.19
    """

    def __init__(self, parent):
        super(DistanceBoxAgent, self).__init__(parent)
        options = [
            {"name": "dis_matrix", "type": "infile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "box_dir", "type": "outfile", "format": "meta.beta_diversity.box_outdir"},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('dis_matrix').is_set:
            raise OptionError('必须提供距离矩阵文件')
        if not self.option('group').is_set:
            raise OptionError('必须提供分组信息文件')
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = ''


class DistanceBoxTool(Tool):
    def __init__(self, config):
        super(DistanceBoxTool, self).__init__(config)
        self._version = '1.9.1'  # qiime版本
        self.cmd_path = 'Python/bin/make_distance_boxplots.py'
        # self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + 'gcc/5.1.0/lib64:$LD_LIBRARY_PATH')
        # 设置运行环境变量

    def run(self):
        """
        运行
        """
        super(DistanceBoxTool, self).run()
        self.run_box()

    def run_box(self):
        """
        运行qiime:make_distance_boxplots.py
        """
        cmd = self.cmd_path
        addline = '#ID\tgroup\n'
        groupfile = open(self.option('group').prop['path'], 'r')
        new = open(os.path.join(self.work_dir, 'temp.gup'), 'w')
        lines = groupfile.readlines()
        new.write(addline)
        for i in lines:
            new.write(i)
        groupfile.close()
        new.close()

        cmd += ' -m %s -d %s -o %s -f "group" --save_raw_data' % (
            os.path.join(self.work_dir, 'temp.gup'), self.option('dis_matrix').prop['path'],
            self.work_dir)
        self.logger.info('运行qiime:make_distance_boxplots.py程序')
        self.logger.info(cmd)
        box_command = self.add_command('box', cmd)
        box_command.run()
        self.wait(box_command)
        if box_command.return_code == 0:
            self.logger.info('运行qiime/make_distance_boxplots.py完成')
            linkfile = self.output_dir + '/group_Distances.txt'
            linkstat = self.output_dir + '/group_Stats.txt'
            if os.path.exists(linkfile):
                os.remove(linkfile)
            if os.path.exists(linkstat):
                os.remove(linkstat)
            os.link(self.work_dir + '/group_Distances.txt', linkfile)
            os.link(self.work_dir + '/group_Stats.txt', linkstat)
            self.option('box_dir', self.output_dir)
            self.end()
        else:
            self.logger.info(box_command.return_code)
            self.set_error('运行qiime/make_distance_boxplots.py出错')
