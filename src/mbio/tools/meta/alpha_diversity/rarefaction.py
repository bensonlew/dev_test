#!/usr/bin/env python
# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import shutil
from biocluster.core.exceptions import OptionError


class RarefactionAgent(Agent):
    """
    rarefaction:稀释曲线
    version 1.0
    author: qindanhua
    last_modify: 2015.11.10
    """
    ESTIMATORS = ['sobs', 'chao', 'ace', 'jack', 'bootstrap', 'simpsoneven',
                  'shannoneven', 'heip', 'smithwilson', 'bergerparker', 'shannon',
                  'npshannon', 'simpson', 'invsimpson', 'coverage', 'qstat']

    def __init__(self, parent):
        super(RarefactionAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table"},  # 输入文件
            {"name": "indices", "type": "string", "default": "chao-shannon"},  # 指数类型
            {"name": "random_number", "type": "int", "default": 100},  # 随机取样数
            {"name": "level", "type": "string", "default": "otu"},  # level水平
            # {"name": "rarefaction", "type": "outfile", "format": "meta.alpha_diversity.rarefaction_dir"}  # 输出结果
        ]
        self.add_option(options)

    def check_options(self):
        """
        检测参数是否正确
        """
        if not self.option("otutable").is_set:
            raise OptionError("请选择otu表")
        if self.option("level") not in ['otu', 'domain', 'kindom', 'phylum', 'class', 'order', 'family', 'genus', 'species']:
            raise OptionError("请选择正确的分类水平")
        for estimators in self.option('indices').split('-'):
            if estimators not in self.ESTIMATORS:
                raise OptionError("请选择正确的指数类型")

    def set_resource(self):
        """
            所需资源
            """
        self._cpu = 10
        self._memory = ''


class RarefactionTool(Tool):
    """
    version 1.0
    """

    def __init__(self, config):
        super(RarefactionTool, self).__init__(config)
        self.cmd_path = '/meta/alpha_diversity/'
        self.shared_path = '/mnt/ilustre/users/sanger/app/meta/scripts/'

    def shared(self):
        """
        执行命令获得shared格式文件，shared文件为下一命令输入文件
        """
        otutable = self.option("otutable").prop['path']
        if self.option("otutable").format is "meta.otu.tax_summary_dir":
            otutable = self.option("otutable").get_table(self.option("level"))
        cmd = os.path.join(self.shared_path, 'otu2shared.pl')
        cmd += ' -i %s -l %s -o %s' % (otutable, '0.97', 'otu.shared')
        # print cmd
        os.system(cmd)

    def mothur(self):
        """
        执行命令运行mothur程序，生成rarefaction结果文件
        """
        cmd = '/meta/mothur.1.30 "#rarefaction.single(shared=otu.shared,calc=sobs-%s,groupmode=f,' \
              'freq=%s,processors=10)"' % (self.option('indices'), self.option('random_number'))
        # print cmd
        self.logger.info("开始运行mothur")
        mothur_command = self.add_command("mothur", cmd)
        mothur_command.run()
        self.wait(mothur_command)
        if mothur_command.return_code == 0:
            self.logger.info("运行mothur完成！")
        else:
            self.set_error("运行mothur出错！")
        self.set_output()

    def set_output(self):
        """
        处理结果文件，将结果文件归类放入相应文件夹并将文件夹连接至output
        """
        self.logger.info("set out put")
        for root, dirs, files in os.walk(self.output_dir):
            for names in dirs:
                shutil.rmtree(os.path.join(self.output_dir, names))
        for estimators in self.option('indices').split('-'):
            cmd = 'mkdir %s|find -name "otu*%s"|xargs mv -t %s' % (estimators, estimators, estimators,)
            os.system(cmd)
            os.system('cp -r %s %s' % (estimators, self.output_dir))
        os.system('mkdir rarefaction|find -name "otu*rarefaction"|xargs mv -t rarefaction')
        os.system('cp -r rarefaction %s' % self.output_dir)
        os.system('mkdir rabund|find -name "otu*rabund"|xargs mv -t rabund')
        os.system('cp -r rabund %s' % self.output_dir)
        # self.option('rarefaction').set_path(self.output_dir+'/rarefaction')
        self.logger.info("done")

    def run(self):
        """
        运行
        """
        super(RarefactionTool, self).run()
        self.shared()
        self.mothur()
        self.end()
        # self.set_output()
