# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import re
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
            {"name": "dis_matrix", "type": "infile",
             "format": "meta.beta_diversity.distance_matrix"},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "grouplabs", "type": "string", "default": ""},
            {"name": "permutations", "type": "int", "default": 999}
        ]
        self.add_option(options)
        self.step.add_steps('anosim_adonis')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.anosim_adonis.start()
        self.step.update()

    def step_end(self):
        self.step.anosim_adonis.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检查

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
            if self.option('grouplabs'):
                if self.option('grouplabs') not in self.option('group').prop['group_scheme']:
                    raise OptionError('选定的分组方案名:%s在分组文件中不存在' % self.option('grouplabs'))
            else:
                self.option('grouplabs', self.option('group').prop['group_scheme'][0])
            if len(samplelist) != len(self.option('group').prop['sample']):
                raise OptionError('分组文件中样本数量：%s与距离矩阵中的样本数量：%s不一致' % (len(self.option('group').prop['sample']),
                                  len(samplelist)))
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
        运行qiime:compare_categories
        """
        cmd = self.cmd_path
        cmd1 = cmd + ' --method anosim -m %s -i %s -o %s -c %s -n %d' % (self.option('group').path,
                                                                         self.option('dis_matrix').prop['path'],
                                                                         self.work_dir, self.option('grouplabs'),
                                                                         self.option('permutations'))
        cmd2 = cmd + ' --method adonis -m %s -i %s -o %s -c %s -n %d' % (self.option('group').path,
                                                                         self.option('dis_matrix').prop['path'],
                                                                         self.work_dir, self.option('grouplabs'),
                                                                         self.option('permutations'))
        self.logger.info('运行qiime:compare_categories.py,计算adonis&anosim程序')
        dist_anosim_command = self.add_command('anosim', cmd1)
        dist_anosim_command.run()
        dist_adonis_command = self.add_command('adonis', cmd2)
        dist_adonis_command.run()
        self.wait()
        if dist_anosim_command.return_code == 0:
            self.logger.info('运行qiime:compare_categories.py计算anosim完成')
            self.linkfile(os.path.join(self.work_dir, 'adonis_results.txt'), 'adonis_results.txt')
            if dist_adonis_command.return_code == 0:
                self.logger.info('运行qiime:compare_categories.py计算adonis完成')
                self.linkfile(os.path.join(self.work_dir, 'anosim_results.txt'), 'anosim_results.txt')
                self.format()
                self.end()
            else:
                self.set_error('运行qiime:compare_categories.py计算adonis出错')
        else:
            self.set_error('运行qiime:compare_categories.py计算anosim出错')

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

    def format(self):
        """
        将‘adonis_results.txt’和‘anosim_results.txt’两个文件的内容
        整理写入到表格‘format_results.xls’中
        """
        an = open(os.path.join(self.output_dir, 'anosim_results.txt'))
        ad = open(os.path.join(self.output_dir, 'adonis_results.txt'))
        new = open(os.path.join(self.output_dir, 'format_results.xls'), 'w')
        an_line = an.readlines()
        ad_r = ''
        ad_p = ''
        for line in ad:
            self.logger.info(line)
            if re.match(r'qiime\.data\$map\[\[opts\$category\]\]', line):
                self.logger.info(line + '--MATCH')
                ad_r = line.split()[5]
                ad_p = line.split()[6]
        an_r = an_line[4].strip().split('\t')[1]
        an_p = an_line[5].strip().split('\t')[1]
        permu = an_line[6].strip().split('\t')[1]
        new.write('method\tstatisic\tp-value\tnumber of permutation\n')
        new.write('anosim\t%s\t%s\t%s\n' % (an_r, an_p, permu))
        new.write('adonis\t%s\t%s\t%s\n' % (ad_r, ad_p, permu))
        new.close()
        ad.close()
        an.close()
