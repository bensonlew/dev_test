# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class AnosimAgent(Agent):
    """
    qiime
    version v1.0
    author: shenghe
    last_modified:2015.11.19
    """

    def __init__(self, parent):
        super(AnosimAgent, self).__init__(parent)
        options = [
            {"name": "dis_matrix", "type": "infile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "anosim_outdir", "type": "outfile", "format": "meta.beta_diversity.anosim_outdir"},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"}
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


class AnosimTool(Tool):

    def __init__(self, config):
        super(AnosimTool, self).__init__(config)
        self._version = '1.9.1'  # qiime版本
        self.cmd_path = 'Python/bin/compare_categories.py'
        # self.set_environ(LD_LIBRARY_PATH = self.config.SOFTWARE_DIR + 'gcc/5.1.0/lib64:$LD_LIBRARY_PATH')


    def run(self):
        """
        运行
        """
        super(AnosimTool, self).run()
        self.run_compare_categories()

    def run_compare_categories(self):
        """
        运行qiime/compare_categories
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
        cmd1 = cmd + ' --method anosim -m %s -i %s -o %s -c "group"' % (
            os.path.join(self.work_dir, 'temp.gup'),
            self.option('dis_matrix').prop['path'],
            self.work_dir)
        cmd2 = cmd + ' --method adonis -m %s -i %s -o %s -c "group"' % (
            os.path.join(self.work_dir, 'temp.gup'),
            self.option('dis_matrix').prop['path'],
            self.work_dir)
        self.logger.info('运行qiime/compare_categories.py,计算adonis/anosim程序')
        dist_anosim_command = self.add_command('anosim', cmd1)
        dist_anosim_command.run()
        self.wait()
        if dist_anosim_command.return_code == 0:
            self.logger.info('运行qiime:compare_categories.py计算anosim完成')
        else:
            self.set_error('运行qiime:compare_categories.py计算anosim出错')
        dist_adonis_command = self.add_command('adonis', cmd2)
        dist_adonis_command.run()
        self.wait()
        if dist_adonis_command.return_code == 0:
            self.logger.info('运行qiime:compare_categories.py计算adonis完成')
            if os.path.exists(os.path.join(self.output_dir, 'adonis_results.txt')):
                os.remove(os.path.join(self.output_dir, 'adonis_results.txt'))
            if os.path.exists(os.path.join(self.output_dir, 'anosim_results.txt')):
                os.remove(os.path.join(self.output_dir, 'anosim_results.txt'))
            os.link(os.path.join(self.work_dir, 'adonis_results.txt'),
                    os.path.join(self.output_dir, 'adonis_results.txt'))
            os.link(os.path.join(self.work_dir, 'anosim_results.txt'),
                    os.path.join(self.output_dir, 'anosim_results.txt'))
            self.option('anosim_outdir', self.output_dir)
            self.option('anosim_outdir').format_result()
            self.end()
        else:
            self.set_error('运行qiime:compare_categories.py计算adonis出错')
