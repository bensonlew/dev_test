#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.module import Module


class AlphaDiversityModule(Module):
    """
    alpha多样性模块
    version 1.0
    author: qindanhua
    last_modify: 2015.11.25
    """
    ESTIMATORS = ['sobs', 'chao', 'ace', 'jack', 'bootstrap', 'simpsoneven', 'shannoneven', 'heip', 'smithwilson',
                  'bergerparker', 'shannon', 'npshannon', 'simpson', 'invsimpson', 'coverage', 'qstat']

    def __init__(self, work_id):
        super(AlphaDiversityModule, self).__init__(work_id)
        options = [
            {"name": "otu_table", "type": "infile", "format": "meta.otu.otu_table,meta.otu.tax_summary_dir"},  # 输入文件
            {"name": "estimate_indices", "type": "string", "format": "ace-chao-shannon-simpson-coverage"},
            {"name": "rarefy_indices", "type": "string", "default": "sobs-shannon"},  # 指数类型
            {"name": "rarefy_freq", "type": "int", "default": 100},
            {"name": "level", "type": "string", "default": "otu"}  # level水平
        ]
        self.add_option(options)
        # self.rank_path = '/mnt/ilustre/users/sanger/app/meta/scripts/'
        self.perl_path = 'Perl/bin/perl'
        self.estimators = self.add_tool('meta.alpha_diversity.estimators')
        self.rarefaction = self.add_tool('meta.alpha_diversity.rarefaction')

    def check_options(self):
        """
        检查参数
        """
        if not self.option("otu_table").is_set:
            raise OptionError("请选择otu表")
        for estimators in self.option('estimate_indices').split('-'):
            if estimators not in self.ESTIMATORS:
                raise OptionError("请选择正确的指数类型")
        for estimators in self.option('rarefy_indices').split('-'):
            if estimators not in self.ESTIMATORS:
                raise OptionError("请选择正确的指数类型")

    def estimators_run(self):
        self.estimators.set_options({
            'otutable': self.option('otu_table'),
            'indices': self.option('estimate_indices'),
            'level': self.option('level')
            })
        # self.on_rely(estimators, self.rarefaction_run)
        self.estimators.run()

    def rarefaction_run(self):
        self.rarefaction.set_options({
            'otutable': self.option('otu_table'),
            'indices': self.option('rarefy_indices'),
            'freq': self.option('rarefy_freq'),
            'level': self.option('level')
            })
        self.rarefaction.on('end', self.set_output)
        self.rarefaction.run()

    def set_output(self):
        self.logger.info('set output')
        for root, dirs, files in os.walk(self.output_dir):
            for names in dirs:
                shutil.rmtree(os.path.join(self.output_dir, names))
            for f in files:
                os.remove(os.path.join(self.output_dir, f))
        estimators = self.work_dir + '/Estimators/output/estimators.xls'
        rarefaction = self.work_dir + '/Rarefaction/output/rarefaction/'
        os.link(estimators, self.output_dir + '/estimators.xls')
        os.system('cp -r %s %s' % (rarefaction, self.output_dir))
        for estimators in self.option('rarefy_indices').split('-'):
            if estimators == "sobs":
                estimators = "rarefaction"
            est_path = self.work_dir + '/Rarefaction/output/%s/' % estimators
            os.system('cp -r %s %s' % (est_path, self.output_dir))
        # self.option('estimators').set_path(self.output_dir+'/estimators')
        # self.option('rarefaction').set_path(self.output_dir+'/rarefaction')
        self.logger.info('done')
        self.end()

    def run(self):
        self.estimators_run()
        self.rarefaction_run()
        self.on_rely([self.estimators, self.rarefaction], self.set_output)
        super(AlphaDiversityModule, self).run()
