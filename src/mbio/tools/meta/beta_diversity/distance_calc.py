# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
from mbio.files.meta.otu.otu_table import OtuTableFile


class DistanceCalcAgent(Agent):
    """
    qiime
    version 1.0
    author shenghe
    last_modified:2016.5.24
    """
    METHOD = ['abund_jaccard', 'binary_chisq', 'binary_chord',
              'binary_euclidean', 'binary_hamming', 'binary_jaccard',
              'binary_lennon', 'binary_ochiai',
              'binary_pearson', 'binary_sorensen_dice',
              'bray_curtis', 'bray_curtis_faith', 'bray_curtis_magurran',
              'canberra', 'chisq', 'chord', 'euclidean', 'gower',
              'hellinger', 'kulczynski', 'manhattan', 'morisita_horn',
              'pearson', 'soergel', 'spearman_approx', 'specprof',
              'unifrac',
              'unweighted_unifrac', 'unweighted_unifrac_full_tree',
              'weighted_normalized_unifrac', 'weighted_unifrac']
    UNIFRACMETHOD = METHOD[-5:]

    def __init__(self, parent):
        super(DistanceCalcAgent, self).__init__(parent)
        options = [
            {"name": "method", "type": "string", "default": "bray_curtis"},
            {"name": "otutable", "type": "infile",
                "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "dis_matrix", "type": "outfile",
             "format": "meta.beta_diversity.distance_matrix"},
            {"name": "newicktree", "type": "infile",
             "format": "meta.beta_diversity.newick_tree"}
        ]
        self.add_option(options)
        self.step.add_steps('distance_calc')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.distance_calc.start()
        self.step.update()

    def step_end(self):
        self.step.distance_calc.finish()
        self.step.update()

    def gettable(self):
        """
        根据level返回进行计算的丰度表-对象
        :return tablepath:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            newtable = OtuTableFile()
            newtable.set_path(self.option('otutable').get_table(self.option('level')))
            return newtable
        else:
            return self.option('otutable')

    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('otutable').is_set:
            raise OptionError('必须提供输入文件')
        else:
            otulist = [line.split('\t')[0] for line in open(self.gettable().prop['path'])][1:]  # 获取所有OTU/物种名
        if self.option('method') in ['binary_otu_gain', 'unifrac_g', 'unifrac_g_full_tree']:
            raise OptionError('算法:%s已经被移除！请使用其他算法！移除时间:2016/4/8 原因: 算法生成不对称矩阵,不利于后续分析 ' % self.option('method'))
        if self.option('method') not in DistanceCalcAgent.METHOD:
            raise OptionError('错误或者不支持的距离矩阵计算方法')
        if self.option('method') in DistanceCalcAgent.UNIFRACMETHOD:
            if not self.option('newicktree').is_set:
                raise OptionError('选择unifrac算法时必须提供newicktree进化树文件')
            else:
                self.option('newicktree').get_info()
                if len(self.option('newicktree').prop['sample']) != len(otulist):
                    raise OptionError('进化树中的类群数量:%s与丰度表中的数量:%s不一致'
                                      % (len(self.option('newicktree').prop['sample']),
                                         len(otulist)))
                tree_sample = self.option('newicktree').prop['sample']
                for sample in otulist:  # 此处可以进一步优化计算，例如将两个列表变成集合
                    if sample not in tree_sample:
                        raise OptionError('丰度表名称:%s与进化树文件中的类群不对应' % sample)

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 5
        self._memory = '10G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "距离矩阵计算结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            [r'%s.*\.xls' % self.option('method'), 'xls', '样本距离矩阵文件']
        ])
        # print self.get_upload_files()
        super(DistanceCalcAgent, self).end()


class DistanceCalcTool(Tool):

    def __init__(self, config):
        super(DistanceCalcTool, self).__init__(config)
        self._version = '1.9.1'  # qiime版本
        self.cmd_path = 'program/Python/bin/beta_diversity.py'
        # 设置运行环境变量
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64')
        self.real_otu = self.gettable()  # 获取真实的丰度表路劲
        self.biom = self.biom_otu_table()  # 传入丰度表需要转化为biom格式

    def run(self):
        """
        运行
        """
        super(DistanceCalcTool, self).run()
        self.run_beta_diversity()

    def gettable(self):
        """
        根据level返回进行计算的丰度表路径
        :return tablepath:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            return self.option('otutable').get_table(self.option('level'))
        else:
            return self.option('otutable').path

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
            self.command_successful()
        elif dist_matrix_command.return_code is None:
            self.logger.warn("运行命令出错，返回值为None，尝试重新运行")
            dist_matrix_command.rerun()
            self.wait()
            if dist_matrix_command.return_code is 0:
                self.command_successful()
            else:
                self.set_error("运行qiime:beta_diversity.py出错")

        else:
            self.set_error('运行qiime:beta_diversity.py出错')

    def command_successful(self):
        self.logger.info('运行qiime:beta_diversity.py完成')
        filename = self.work_dir + '/' + \
            self.option('method') + '_temp.txt'
        basename = os.path.splitext(os.path.basename(self.real_otu))[0]
        linkfile = self.output_dir + '/' + \
            self.option('method') + '_' + basename + '.xls'
        if os.path.exists(linkfile):
            os.remove(linkfile)
        os.link(filename, linkfile)
        # self.option('dis_matrix').set_path(linkfile)
        self.option('dis_matrix', linkfile)
        self.end()

    def biom_otu_table(self):
        """
        将otutable转化成biom格式
        :return biom_path:返回生成的biom文件路径
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            # 如果提供的是otu分类文件夹，需要重新创建类，再使用类的convert_to_biom方法
            newtable = OtuTableFile()
            newtable.set_path(self.real_otu)
            newtable.check()
        else:
            newtable = self.option('otutable')
        newtable.get_info()
        biom_path = os.path.join(self.work_dir, 'temp.biom')
        if os.path.isfile(biom_path):
            os.remove(biom_path)
        newtable.convert_to_biom(biom_path)
        return biom_path
