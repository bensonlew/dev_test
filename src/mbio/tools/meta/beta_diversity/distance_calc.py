# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
from mbio.files.otu_table import OtuTableFile


class DistanceCalcAgent(Agent):
    """
    qiime
    version 1.0
    author shenghe
    last_modified:2015.11.5
    """
    METHOD = ['abund_jaccard', 'binary_chisq', 'binary_chord',
              'binary_euclidean', 'binary_hamming', 'binary_jaccard',
              'inary_lennon', 'binary_ochiai', 'binary_otu_gain',
              'binary_pearson', 'binary_sorensen_dice',
              'bray_curtis', 'bray_curtis_faith', 'bray_curtis_magurran',
              'canberra', 'chisq', 'chord', 'euclidean', 'gower',
              'hellinger', 'kulczynski', 'manhattan', 'morisita_horn',
              'pearson', 'soergel', 'spearman_approx', 'specprof',
              'unifrac', 'unifrac_g', 'unifrac_g_full_tree',
              'unweighted_unifrac', 'unweighted_unifrac_full_tree',
              'weighted_normalized_unifrac', 'weighted_unifrac']
    UNIFRACMETHOD = ['unifrac', 'unifrac_g', 'unifrac_g_full_tree',
                     'unweighted_unifrac', 'unweighted_unifrac_full_tree',
                     'weighted_normalized_unifrac', 'weighted_unifrac']

    def __init__(self, parent):
        super(DistanceCalcAgent, self).__init__(parent)
        options = [
            {"name": "method", "type": "string", "default": "bray_curtis"},
            {"name": "otutable", "type": "infile", "format": ".meta.otu.otu_table"},
            {"name": "dis_matrix", "type": "outfile", "format": ".meta.beta_diversity.distance_matrix"},
            {"name": "newicktree", "type": "infile", "format": "meta.beta_diversity.newick_tree"},
        ]
        self.add_option(options)

    def check_options(self):
        """
        重写参数检查
        :return:
        """
        if not self.option('input1').is_set:
            raise OptionError('必须提供输入文件')
        if not self.option('output').is_set:
            raise OptionError('必须提供输出文件名')
        if self.option('method').value in DistanceCalcAgent.METHOD:
            pass
        else:
            raise OptionError('错误或者不支持的距离矩阵计算方法')
        if self.option('method').value in DistanceCalcAgent.UNIFRACMETHOD:
            if not self.option('input2').is_set:
                raise OptionError('选择unifrac算法时必须提供newicktree进化树文件')
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2  # 目前还不知道哪些距离计算消耗较多
        self._memory = ''


class DistanceCalcTool(Tool):

    def __init__(self, config):
        super(DistanceCalcTool, self).__init__(config)
        self._version = '1.9.1'  # qiime版本
        self.cmd_path = '/mnt/ilustre/lustre2-bak/share/src/qiime_software/qiime-1.8.0-release/scripts/beta_diversity.py'
        # 安装位置不确定，待定
        self.biom_otu_table()  # 传入的参数otu表是普通文本格式，需要转化为biom格式

    def run(self):
        """
        运行
        :return:
        """
        super(DistanceCalcTool, self).run()
        self.run_beta_diversity()

    def run_beta_diversity(self):
        """
        运行qiime/beta_diversity
        :return:
        """
        outputdir = os.path.dirname(self.option('output'))
        filename = os.path.basename(self.option('output'))
        cmd = self.cmd_path
        if self.option('method') in DistanceCalcAgent.UNIFRACMETHOD:
            cmd += ' -m %s -i %s -o %s -t %s' % (
                self.option('method'), self.option('input1'),
                outputdir, self.option('input2'))
        else:
            cmd += ' -m %s -i %s -o %s' % (
                self.option('method'), self.option('input1'),
                outputdir)
        self.logger.info('运行qiime/beta_diversity.py程序')
        dist_matrix_command = self.add_command('distance_matrix', cmd)
        dist_matrix_command.run()
        self.wait()
        if dist_matrix_command.return_code == 0:
            self.logger.info('运行qiime/beta_diversity.py完成')
            oldfile = (outputdir + '/' + self.option('method') + '_' +
                       os.path.splitext(os.path.basename(
                           self.option('input1')))[0]
                       + '.txt')
            newfile = outputdir + '/' + self.option('method') + '_' + filename
            os.rename(oldfile, newfile)
            self.end()
        else:
            self.set_error('运行qiime/beta_diversity.py出错')

    def biom_otu_table(self):
        """
        将otutable转化成biom格式
        :return:
        """
        temp = OtuTableFile()
        temp.convert_to_biom((self.option('input1') + '.biom'))
        self.option('input1', (self.option('input1') + '.biom'))
