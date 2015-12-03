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
            {"name": "otu_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "indices", "type": "string", "format": "ace-chao-shannon-simpson-coverage"},
            {"name": "random_number", "type": "int", "default": 100}
            # {"name": "rarefaction", "type": "outfile", "format": "meta.alpha_diversity.rarefaction_dir"},
            # {"name": "estimators", "type": "outfile", "format": "meta.alpha_diversity.estimators"}
        ]
        self.add_option(options)
        self.rank_path = '/mnt/ilustre/users/sanger/app/meta/scripts/'
        self.perl_path = '/mnt/ilustre/users/sanger/app/Perl/bin/perl'

    def check_options(self):
        """
        检查参数
        """
        if not self.option("otu_table").is_set:
            raise OptionError("请选择otu表")
        for estimators in self.option('indices').split('-'):
            if estimators not in self.ESTIMATORS:
                raise OptionError("请选择正确的指数类型")

    def estimators_run(self):
        estimators = self.add_tool('meta.alpha_diversity.estimators')
        estimators.set_options({'otutable': self.option('otu_table'), 'indices': self.option('indices')})
        self.on_rely(estimators, self.rarefaction_run)
        estimators.run()

    def rarefaction_run(self):
        rarefaction = self.add_tool('meta.alpha_diversity.rarefaction')
        rarefaction.set_options({'otutable': self.option('otu_table'), 'indices': self.option('indices'),
                                'random_number': self.option('random_number')})
        rarefaction.on('end', self.set_output)
        rarefaction.run()

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
        for estimators in self.option('indices').split('-'):
            est_path = self.work_dir + '/Rarefaction/output/%s/' % estimators
            os.system('cp -r %s %s' % (est_path, self.output_dir))
        # self.option('estimators').set_path(self.output_dir+'/estimators')
        # self.option('rarefaction').set_path(self.output_dir+'/rarefaction')
        self.logger.info('done')
        self.end()

    def run(self):
        self.estimators_run()
        super(AlphaDiversityModule, self).run()
