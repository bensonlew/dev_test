# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class DistanceCalcAgent(Agent):
    """
    qiime
    version 1.0
    author shenghe
    last_modified:2015.11.5
    """
    METHOD = ['abund_jaccard', 'binary_chisq', 'binary_chord',
              'binary_euclidean', 'binary_hamming', 'binary_jaccard',
              'binary_lennon', 'binary_ochiai', 'binary_otu_gain',
              'binary_pearson', 'binary_sorensen_dice',
              'bray_curtis', 'bray_curtis_faith', 'bray_curtis_magurran',
              'canberra', 'chisq', 'chord', 'euclidean', 'gower',
              'hellinger', 'kulczynski', 'manhattan', 'morisita_horn',
              'pearson', 'soergel', 'spearman_approx', 'specprof',
              'unifrac', 'unifrac_g', 'unifrac_g_full_tree',
              'unweighted_unifrac', 'unweighted_unifrac_full_tree',
              'weighted_normalized_unifrac', 'weighted_unifrac']
    UNIFRACMETHOD = METHOD[-7:]

    def __init__(self, parent):
        super(DistanceCalcAgent, self).__init__(parent)
        options = [
            {"name": "method", "type": "string", "default": "bray_curtis"},
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "dis_matrix", "type": "outfile",
             "format": "meta.beta_diversity.distance_matrix"},
            {"name": "newicktree", "type": "infile",
             "format": "meta.beta_diversity.newick_tree"}
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('otutable').is_set:
            raise OptionError('必须提供输入文件')
        if self.option('method') not in DistanceCalcAgent.METHOD:
            raise OptionError('错误或者不支持的距离矩阵计算方法')
        if self.option('method') in DistanceCalcAgent.UNIFRACMETHOD:
            if not self.option('newicktree').is_set:
                raise OptionError('选择unifrac算法时必须提供newicktree进化树文件')

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 5
        self._memory = ''


class DistanceCalcTool(Tool):
    def __init__(self, config):
        super(DistanceCalcTool, self).__init__(config)
        self._version = '1.9.1'  # qiime版本
        self.cmd_path = 'Python/bin/beta_diversity.py'
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + 'gcc/5.1.0/lib64:$LD_LIBRARY_PATH')
        # 设置运行环境变量
        self.biom = self.biom_otu_table()  # 传入otu表需要转化为biom格式

    def run(self):
        """
        运行
        """
        super(DistanceCalcTool, self).run()
        self.run_beta_diversity()

    def run_beta_diversity(self):
        """
        运行qiime:beta_diversity.py
        """
        cmd = self.cmd_path
        cmd += ' -m %s -i %s -o %s' % (self.option('method'), self.biom,
                                       self.work_dir)
        if self.option('method') in DistanceCalcAgent.UNIFRACMETHOD:
            cmd += ' -t %s' % (self.option('newicktree').prop['path'])
        self.logger.info('运行qiime:beta_diversity.py程序')
        self.logger.info(cmd)
        dist_matrix_command = self.add_command('distance_matrix', cmd)
        dist_matrix_command.run()
        self.wait()
        if dist_matrix_command.return_code == 0:
            self.logger.info('运行qiime:beta_diversity.py完成')
            filename = self.work_dir + '/' + self.option('method') + '_temp.txt'
            basename = os.path.splitext(os.path.basename(self.option('otutable').prop['path']))[0]
            linkfile = self.output_dir + '/' + self.option('method') + '_' + basename + '.txt'
            if os.path.exists(linkfile):
                os.remove(linkfile)
            os.link(filename, linkfile)
            self.option('dis_matrix').set_path(linkfile)
            self.end()
        else:
            self.set_error('运行qiime:beta_diversity.py出错')

    def biom_otu_table(self):
        """
        将otutable转化成biom格式
        :return biom_path:返回生成的biom文件路径
        """
        self.option('otutable').check()
        biom_path = os.path.join(self.work_dir, 'temp.biom')
        if os.path.isfile(biom_path):
            os.remove(biom_path)
        self.option('otutable').convert_to_biom(biom_path)
        return biom_path
