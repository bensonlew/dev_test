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
    last_modify: 2015.12.29
    """
    ESTIMATORS_E = ['ace', 'bergerparker', 'boneh', 'bootstrap', 'bstick', 'chao', 'coverage', 'default', 'efron',
                    'geometric', 'goodscoverage', 'heip', 'invsimpson', 'jack', 'logseries', 'npshannon', 'nseqs',
                    'qstat', 'shannon', 'shannoneven', 'shen', 'simpson', 'simpsoneven', 'smithwilson', 'sobs', 'solow']
    ESTIMATORS_R = ['ace', 'bootstrap', 'chao', 'coverage', 'default', 'heip', 'invsimpson', 'jack', 'npshannon',
                    'nseqs', 'shannon', 'shannoneven', 'simpson', 'simpsoneven', 'smithwilson', 'sobs']

    def __init__(self, work_id):
        super(AlphaDiversityModule, self).__init__(work_id)
        options = [
            {"name": "otu_table", "type": "infile", "format": "meta.otu.otu_table,meta.otu.tax_summary_dir"},  # 输入文件
            {"name": "estimate_indices", "type": "string", "default": "ace,chao,shannon,simpson,coverage"},
            {"name": "rarefy_indices", "type": "string", "default": "sobs,shannon"},  # 指数类型
            {"name": "rarefy_freq", "type": "int", "default": 100},
            {"name": "level", "type": "string", "default": "otu"}  # level水平
        ]
        self.add_option(options)
        # self.rank_path = '/mnt/ilustre/users/sanger/app/meta/scripts/'
        self.perl_path = 'Perl/bin/perl'
        self.estimators = self.add_tool('meta.alpha_diversity.estimators')
        self.rarefaction = self.add_tool('meta.alpha_diversity.rarefaction')
        self.step.add_steps('estimators', 'rarefaction')

    def check_options(self):
        """
        检查参数
        """
        if not self.option("otu_table").is_set:
            raise OptionError("请选择otu表")
        for estimators in self.option('estimate_indices').split(','):
            if estimators not in self.ESTIMATORS_E:
                raise OptionError("请选择正确的指数类型")
        for estimators in self.option('rarefy_indices').split(','):
            if estimators not in self.ESTIMATORS_R:
                raise OptionError("请选择正确的指数类型")

    def estimators_run(self):
        self.estimators.set_options({
            'otu_table': self.option('otu_table'),
            'indices': self.option('estimate_indices'),
            'level': self.option('level')
            })
        # self.on_rely(estimators, self.rarefaction_run)
        self.step.estimators.start()
        self.estimators.on("end", self.finish_update)
        self.estimators.run()
        # self.on_rely(self.estimators, self.finish_update)

    def finish_update(self):
        self.step.estimators.finish()
        self.step.update()

    def rare_finish_update(self):
        self.step.rarefaction.finish()
        self.step.update()

    def rarefaction_run(self):
        self.rarefaction.set_options({
            'otu_table': self.option('otu_table'),
            'indices': self.option('rarefy_indices'),
            'freq': self.option('rarefy_freq'),
            'level': self.option('level')
            })
        # self.rarefaction.on('end', self.set_output)
        self.step.rarefaction.start()
        self.rarefaction.on("end", self.rare_finish_update)
        self.rarefaction.run()
        # self.on_rely(self.rarefaction, self.finish_update)

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
        for estimators in self.option('rarefy_indices').split(','):
            if estimators == "sobs":
                estimators = "rarefaction"
            est_path = self.work_dir + '/Rarefaction/output/%s/' % estimators
            os.system('cp -r %s %s' % (est_path, self.output_dir))
        # self.option('estimators').set_path(self.output_dir+'/estimators')
        # self.option('rarefaction').set_path(self.output_dir+'/rarefaction')
        self.logger.info('done')
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ["./estimators.xls", "xls", "alpha多样性指数表"]
        ])
        for i in self.option("rarefy_indices").split(","):
            self.logger.info(i)
            if i == "sobs":
                result_dir.add_relpath_rules([
                    ["./rarefaction", "文件夹", "{}指数结果输出目录".format(i)]
                ])
                result_dir.add_regexp_rules([
                    [r".*rarefaction\.xls", "xls", "{}指数的simpleID的稀释性曲线表".format(i)]
                ])
                # self.logger.info("{}指数的simpleID的稀释性曲线表".format(i))
            else:
                result_dir.add_relpath_rules([
                    ["./{}".format(i), "文件夹", "{}指数结果输出目录".format(i)]
                ])
                result_dir.add_regexp_rules([
                    [r".*{}\.xls".format(i), "xls", "{}指数的simpleID的稀释性曲线表".format(i)]
                ])
        print self.get_upload_files()
        self.end()

    def run(self):
        super(AlphaDiversityModule, self).run()
        self.estimators_run()
        self.rarefaction_run()
        self.on_rely([self.estimators, self.rarefaction], self.set_output)
