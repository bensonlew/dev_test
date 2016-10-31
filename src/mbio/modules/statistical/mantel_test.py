# -*- coding: utf-8 -*-
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
import os


class MantelTestModule(Module):
    """
    module for mantel test
    author: wangbixuan
    last modified : 20160913 by qindanhua
    """
    MATRIX = ['abund_jaccard', 'binary_chisq', 'binary_chord', 'binary_euclidean', 'binary_hamming', 'binary_jaccard',
              'binary_lennon', 'binary_ochiai', 'binary_otu_gain', 'binary_pearson', 'binary_sorensen_dice',
              'bray_curtis', 'bray_curtis_faith', 'bray_curtis_magurran', 'canberra', 'chisq', 'chord', 'euclidean',
              'gower', 'hellinger', 'kulczynski', 'manhattan', 'morisita_horn', 'pearson', 'soergel', 'spearman_approx',
              'specprof', 'unifrac', 'unweighted_unifrac', 'weighted_normalized_unifrac', 'weighted_unifrac']

    MATRIXFACTOR = ['abund_jaccard', 'binary_chisq', 'binary_chord', 'binary_euclidean', 'binary_hamming',
                    'binary_jaccard', 'binary_lennon', 'binary_ochiai', 'binary_otu_gain', 'binary_pearson',
                    'binary_sorensen_dice', 'bray_curtis', 'bray_curtis_faith', 'bray_curtis_magurran', 'canberra',
                    'chisq', 'chord', 'euclidean', 'gower', 'hellinger', 'kulczynski', 'manhattan', 'morisita_horn',
                    'pearson', 'soergel', 'spearman_approx', 'specprof']

    def __init__(self, work_id):
        super(MantelTestModule, self).__init__(work_id)
        options = [
            {"name": "level", "type": "string", "default": "otu"},  # 分类水平
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "otumatrixtype", "type": "string", "default": "weighted_unifrac"},  # 计算群落距离矩阵的方法
            {"name": "factor", "type": "infile", "format": "meta.otu.group_table"},  # 环境因子
            {"name": "partial_factor", "type": "string"},  # 控制单位
            {"name": "factormatrixtype", "type": "string", "default": "bray_curtis"},  # 计算群落距离矩阵的方法
            {"name": "factorselected", "type": "string", "default": ""},
            {"name": "newicktree", "type": "infile", "format": "meta.beta_diversity.newick_tree"},
            {"name": "partialmatrix", "type": "infile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "dis_matrix", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "fac_matrix", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"}
        ]
        self.otudistance = self.add_tool('meta.beta_diversity.distance_calc')
        self.facdistance = self.add_tool('statistical.factor_distance')
        self.discomparison = self.add_tool('statistical.discomparison')
        self.partial = self.add_tool('statistical.factor_distance')
        self.add_option(options)
        self.step.add_steps('otudistance', 'facdistance', 'discomparison', 'partial')

    def gettable(self):
        """
        get matrix for calculation by level provided
        """
        if self.option("otutable").format == "meta.otu.tax_summary_dir":
            return self.option("otutable").get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']

    def check_options(self):
        if self.option("level") not in ['otu', 'domain', 'kindom', 'phylum', 'class', 'order', 'family', 'genus', 'species']:
            raise OptionError("分类水平不正确")
        if not self.option("otutable").is_set:
            raise OptionError('必须提供otu表')
        self.option('otutable').get_info()
        if not self.option("factor").is_set:
            raise OptionError('必须提供环境因子表')
        else:
            self.option('factor').get_info()
            partial_factors = self.option('partial_factor').split(',')
            for f in partial_factors:
                if f not in self.option('factor').prop['group_scheme']:
                    raise OptionError('该因子不存在于环境因子表：%s' %f)
            if self.option('factorselected'):
                factors = self.option('factorselected').split(',')
                for f in factors:
                    if f not in self.option('factor').prop['group_scheme']:
                        raise OptionError('该因子不存在于环境因子表：%s' %f)
            else:
                pass
        if self.option("otumatrixtype") not in MantelTestModule.MATRIX:
            raise OptionError('otu距离矩阵计算方法不正确')
        if self.option("factormatrixtype") not in MantelTestModule.MATRIXFACTOR:
            raise OptionError('环境因子距离矩阵计算方法不正确')
        if "unifrac" in self.option("otumatrixtype"):
            if not self.option("newicktree").is_set:
                raise OptionError("unifrac方法必须提供newicktree")
        if self.option("newicktree").is_set:
            self.option("newicktree").get_info()

    def otudistance_run(self):
        options = {
            'otutable': self.option('otutable'),
            'level': self.option('level'),
            'method': self.option('otumatrixtype')
            }
        if self.option('newicktree').is_set:
            options["newicktree"] = self.option('newicktree')
        self.otudistance.set_options(options)
        self.step.otudistance.start()
        self.otudistance.on("end", self.set_output, 'otudistance')
        # self.otudistance.on("end",self.facdistance_run)
        self.otudistance.run()

    def facdistance_run(self):
        options = {
            'factor': self.option('factor'),
            'facmatrixtype': self.option('factormatrixtype')
            }
        if self.option('factorselected'):
            options['factorselected'] = self.option('factorselected')
        self.facdistance.set_options(options)
        self.step.facdistance.start()
        self.facdistance.on("end", self.set_output, 'facdistance')
        # self.facdistance.on("end",self.discomparison_run)
        self.facdistance.run()

    def partial_run(self):
        options = {
            'factor': self.option('factor'),
            'facmatrixtype': self.option('factormatrixtype'),
            'factorselected': self.option('partial_factor')
            }
        self.partial.set_options(options)
        self.step.partial.start()
        self.partial.on("end", self.set_output, 'partial')
        # self.facdistance.on("end",self.discomparison_run)
        self.partial.run()

    def discomparison_run(self):
        options = {
            'otudistance':  self.otudistance.option('dis_matrix'),
            'facdistance': self.facdistance.option('fac_matrix')
            }
        if self.option('partial_factor'):
            options['partialmatrix'] = self.option('partialmatrix')
        self.discomparison.set_options(options)
        self.step.discomparison.start()
        self.discomparison.on("end", self.set_output, 'discompare')
        self.discomparison.run()

    def run(self):
        super(MantelTestModule, self).run()
        self.otudistance_run()
        self.step.update()
        self.facdistance_run()
        self.step.update()
        if self.option("partial_factor"):
            print("lllllllllllll")
            self.partial_run()
            self.step.update()
            self.on_rely([self.otudistance, self.facdistance, self.partial], self.discomparison_run)
        else:
            self.on_rely([self.otudistance, self.facdistance], self.discomparison_run)
        # self.on_rely(self.discomparison_run,self.end)
        self.discomparison.on("end", self.end)

    def set_output(self, event):
        obj = event['bind_object']
        if event['data'] == 'facdistance':
            self.linkdir(obj.output_dir, 'Facdistance')
            self.step.facdistance.finish()
        elif event['data'] == 'otudistance':
            self.linkdir(obj.output_dir, 'Otudistance')
            self.step.otudistance.finish()
        elif event['data'] == 'discompare':
            self.linkdir(obj.output_dir, 'Discompare')
            self.step.discomparison.finish()
        elif event['data'] == 'partial':
            self.linkdir(obj.output_dir, 'partial')
            self.option('partialmatrix').set_path(os.path.join(obj.output_dir, "factor_out.xls"))
            self.step.discomparison.finish()
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

    def stepend(self):
        self.step.update()
        self.end()

    def end(self):
        repaths = [
            [".", "", "Mantel_test计算结果文件目录"],
            ["otu_distance.xls", "xls", "样本距离矩阵文件"],
            ["factor_distance.xls", "xls", "环境因子距离矩阵文件"],
            ["mantel_results.txt", "txt", "Discomparison结果"]
        ]
        regexps = [
            [r'%s.*\.xls' % self.option('otumatrixtype'), 'xls', '样本距离矩阵文件'],
            [r'%s.*\.xls' % self.option('factormatrixtype'), 'xls', '环境因子距离矩阵文件'],
            ["./mantel_results.txt", "txt", "Discomparison结果"]
        ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        super(MantelTestModule, self).end()
