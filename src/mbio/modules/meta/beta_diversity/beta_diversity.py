# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

from biocluster.module import Module
import os
from biocluster.core.exceptions import OptionError


class BetaDiversityModule(Module):

    def __init__(self, work_id):
        super(BetaDiversityModule, self).__init__(work_id)
        self.step.add_steps('ChooseAnalysis', 'MultipleAnalysis')
        options = [
            {"name": "analysis", "type": "string",
                "default": "anosim,pca,pcoa,nmds,rda_cca,dbrda,hcluster"},
            {"name": "dis_method", "type": "string", "default": "bray_curtis"},
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "phy_newick", "type": "infile",
             "format": "meta.beta_diversity.newick_tree"},
            {"name": "permutations", "type": "int", "default": 999},
            {"name": "linkage", "type": "string", "default": "average"},
            {"name": "envtable", "type": "infile", "format": "meta.env_table"},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "dis_matrix", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "dis_newicktree", "type": "outfile", "format": "meta.beta_diversity.newick_tree"}
        ]
        self.add_option(options)
        self.matrix = self.add_tool('meta.beta_diversity.distance_calc')
        self.tools = {}

    def gettable(self):
        """
        根据level返回进行计算的otu表
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            return self.option('otutable').get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']

    def check_options(self):
        if self.option('permutations') < 0 or self.option('permutations') > 10000:
            raise OptionError('参数permutations：%s 不在范围内(0-10000)' %
                              self.option('permutations'))
        samplelist = open(self.gettable()).readline().strip().split('\t')[1:]
        if self.option('linkage') not in ['average', 'single', 'complete']:
            raise OptionError('错误的层级聚类方式：%s' % self.option('linkage'))
        if 'rda_cca' in self.option('analysis') and not self.option('envtable').is_set:
            raise OptionError('计算RDA/CCA需要环境因子表')
        if ('anosim' or 'rda_cca' or 'dbrda') in self.option('analysis') and not self.option('group').is_set:
            raise OptionError('分析需要相关分组文件')
        else:
            if ('anosim' or 'rda_cca' or 'dbrda') in self.option('analysis'):
                self.option('group').get_info()
                for sample in self.option('group').prop['sample']:
                    if sample not in samplelist:
                        raise OptionError('分组文件的样本(%s)在otu表的样本中不存在' % sample)
        return True

    def matrix_run(self):
        """
        运行计算距离矩阵
        :return:
        """
        if self.option('phy_newick').is_set:
            self.matrix.set_options({'method': self.option('dis_method'),
                                     'otutable': self.gettable(),
                                     'newicktree': self.option('phy_newick')})
        else:
            self.matrix.set_options({'method': self.option('dis_method'),
                                     'otutable': self.gettable()})
        self.matrix.on('end', self.set_output, 'distance')
        self.matrix.run()

    def hcluster_run(self, rely_obj):
        output_file_obj = rely_obj.rely[0].option('dis_matrix')
        self.tools['hcluster'].set_options({
            'dis_matrix': output_file_obj,
            'linkage': self.option('linkage')
        })
        self.tools['hcluster'].on('end', self.set_output, 'hcluster')
        self.tools['hcluster'].run()

    def anosim_run(self, rely_obj):
        output_file_obj = rely_obj.rely[0].option('dis_matrix')
        self.tools['anosim'].set_options({
            'dis_matrix': output_file_obj,
            'group': self.option('group'),
            'permutations': self.option('permutations')
        })
        self.tools['anosim'].on('end', self.set_output, 'anosim')
        self.tools['anosim'].run()

    def box_run(self, rely_obj):
        output_file_obj = rely_obj.rely[0].option('dis_matrix')
        self.tools['box'].set_options({
            'dis_matrix': output_file_obj,
            'group': self.option('group')
        })
        self.tools['box'].on('end', self.set_output, 'box')
        self.tools['box'].run()

    def pcoa_run(self, rely_obj):
        output_file_obj = rely_obj.rely[0].option('dis_matrix')
        self.tools['pcoa'].set_options({
            'dis_matrix': output_file_obj
        })
        self.tools['pcoa'].on('end', self.set_output, 'pcoa')
        self.tools['pcoa'].run()

    def nmds_run(self, rely_obj):
        output_file_obj = rely_obj.rely[0].option('dis_matrix')
        self.tools['nmds'].set_options({
            'dis_matrix': output_file_obj
        })
        self.tools['nmds'].on('end', self.set_output, 'nmds')
        self.tools['nmds'].run()

    def dbrda_run(self, rely_obj):
        output_file_obj = rely_obj.rely[0].option('dis_matrix')
        self.tools['dbrda'].set_options({
            'dis_matrix': output_file_obj,
            'group': self.option('group')
        })
        self.tools['dbrda'].on('end', self.set_output, 'dbrda')
        self.tools['dbrda'].run()

    def rda_run(self):
        self.tools['rda'].set_options({
            'otutable': self.gettable(),
            'envtable': self.option('envtable')
        })
        self.tools['rda'].on('end', self.set_output, 'rda')
        self.tools['rda'].run()

    def pca_run(self):
        if self.option('envtable').is_set:
            self.tools['pca'].set_options({
                'otutable': self.gettable(),
                'envtable': self.option('envtable')})
        else:
            self.tools['pca'].set_options({
                'otutable': self.gettable()})
        self.tools['pca'].on('end', self.set_output, 'pca')
        self.tools['pca'].run()

    def set_output(self, event):
        obj = event['bind_object']
        if event['data'] == 'pca':
            self.linkdir(obj.output_dir, 'Pca')
        elif event['data'] == 'rda':
            self.linkdir(obj.output_dir, 'Rda')
        elif event['data'] == 'distance':
            self.linkdir(obj.output_dir, 'Distance')
            self.option('dis_matrix', obj.option('dis_matrix'))
        elif event['data'] == 'hcluster':
            self.linkdir(obj.output_dir, 'Hcluster')
            self.option('dis_newicktree', obj.option('newicktree'))
        elif event['data'] == 'anosim':
            self.linkdir(obj.output_dir, 'Anosim')
        elif event['data'] == 'box':
            self.linkdir(obj.output_dir, 'Box')
        elif event['data'] == 'dbrda':
            self.linkdir(obj.output_dir, 'Dbrda')
        elif event['data'] == 'pcoa':
            self.linkdir(obj.output_dir, 'Pcoa')
        elif event['data'] == 'nmds':
            self.linkdir(obj.output_dir, 'Nmds')
        else:
            pass

    def linkdir(self, dirpath, dirname):
        """
        link一个文件夹下的所有文件到本module的output目录
        :param dirpath: 传入文件夹路径
        :param dirname: 新的文件夹名称
        :return:
        """
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                os.remove(newfile)
        for i in range(len(allfiles)):
            os.link(oldfiles[i], newfiles[i])

    def run(self):
        super(BetaDiversityModule, self).run()
        self.step.ChooseAnalysis.start()
        self.step.update()
        if 'anosim' in self.option('analysis'):
            self.tools['anosim'] = self.add_tool('meta.beta_diversity.anosim')
            self.on_rely(self.matrix, self.anosim_run)
            self.tools['box'] = self.add_tool(
                'meta.beta_diversity.distance_box')
            self.on_rely(self.matrix, self.box_run)
        if 'pcoa' in self.option('analysis'):
            self.tools['pcoa'] = self.add_tool('meta.beta_diversity.pcoa')
            self.on_rely(self.matrix, self.pcoa_run)
        if 'nmds' in self.option('analysis'):
            self.tools['nmds'] = self.add_tool('meta.beta_diversity.nmds')
            self.on_rely(self.matrix, self.nmds_run)
        if 'dbrda' in self.option('analysis'):
            self.tools['dbrda'] = self.add_tool('meta.beta_diversity.dbrda')
            self.on_rely(self.matrix, self.dbrda_run)
        if 'hcluster' in self.option('analysis'):
            self.tools['hcluster'] = self.add_tool(
                'meta.beta_diversity.hcluster')
            self.on_rely(self.matrix, self.hcluster_run)
        if self.tools or 'distance_matrix' in self.option('analysis'):
            self.matrix_run()
        if 'pca' in self.option('analysis'):
            self.tools['pca'] = self.add_tool('meta.beta_diversity.pca')
            self.pca_run()
        if 'rda_cca' in self.option('analysis'):
            self.tools['rda'] = self.add_tool('meta.beta_diversity.rda_cca')
            self.rda_run()
        self.step.ChooseAnalysis.finish()
        self.step.MultipleAnalysis.start()
        self.step.update()
        self.on_rely(self.tools.values(), self.stepend)
        self.on_rely(self.tools.values(), self.end)

    def stepend(self):
        self.step.MultipleAnalysis.finish()
        self.step.update()