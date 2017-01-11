# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

"""beta多元分析"""

import os
import re
import numpy as np
import types
from biocluster.workflow import Workflow
from mbio.packages.beta_diversity.filter_newick import get_level_newicktree


class BetaMultiAnalysisWorkflow(Workflow):
    """
    报告中调用beta多元分析
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(BetaMultiAnalysisWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "analysis_type", "type": "string", "default": 'pca'},
            {"name": "dist_method", "type": "string", "default": 'bray_curtis'},
            {"name": "update_info", "type": "string"},
            {"name": "otu_id", "type": "string"},
            {"name": "main_id", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "multi_analysis_id", "type": "string"},
            {"name": "env_file", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "group_file", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "env_labs", "type": "string", "default": ""},
            {"name": "group_detail", "type": "string", "default": ""},
            {"name": "group_id", "type": "string", "default": ""},
            {"name": "env_id", "type": "string", "default": ""},
            {"name": "params", "type": "string", "default": ""},
            # {"name": "matrix_out", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"}
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def run(self):
        task = self.add_module("meta.beta_diversity.beta_diversity")
        self.logger.info(self.option('otu_file').path)
        options = {
            'analysis': self.option('analysis_type'),
            # 'dis_method': self.option('dist_method'),
            'otutable': self.option('otu_file')

            }
        if self.option('env_file').is_set:
            options['envlabs'] = self.option('env_labs')
            options['envtable'] = self.option('env_file')
        else:
            pass
        if self.option('analysis_type') == 'plsda':
            options['group'] = self.option('group_file')
            options['grouplab'] = self.option('group_file').prop['group_scheme'][0]
        if self.option('analysis_type') in ['pcoa', 'nmds', 'dbrda']:
            options['dis_method'] = self.option('dist_method')
            if 'unifrac' in self.option('dist_method'):  # sanger_bioinfo/src/mbio/workflows/meta/report/distance_calc.py中的解释
                if self.option('level') != 9:
                    newicktree = get_level_newicktree(self.option('otu_id'), level=self.option('level'),
                                                      tempdir=self.work_dir, return_file=False, bind_obj=self)
                    all_find = re.findall(r'\'.+?\'', newicktree)
                    for n, m in enumerate(all_find):
                        all_find[n] = m.strip('\'')
                    all_find = dict((i[1], i[0]) for i in enumerate(all_find))

                    def match_newname(matchname):
                        if hasattr(match_newname, 'count'):
                            match_newname.count = match_newname.count + 1
                        else:
                            match_newname.count = 1
                        return 'OTU' + str(match_newname.count)
                    newline = re.sub(r'\'.+?\'', match_newname, newicktree)
                    temp_tree_file = self.work_dir + '/temp.tree'
                    tempfile = open(temp_tree_file, 'w')
                    tempfile.write(newline)
                    tempfile.close()
                    self.logger.info('get_newick:' + temp_tree_file)
                    otu_table = self.option('otu_file').path
                    temp_otu_file = self.option('otu_file').path + '.temp'
                    all_lines = open(otu_table, 'r').readlines()
                    if len(all_lines) < 3:
                        raise Exception('分类水平：%s,otu表数据少于2行：%s' % (self.option('level'), len(all_lines)))
                    self.logger.info(len(all_lines))
                    new_all = []
                    new_all.append(all_lines[0])
                    for line in all_lines[1:]:
                        name = line.split('\t')
                        if name[0] in all_find:
                            name[0] = 'OTU' + str(all_find[name[0]] + 1)
                        new_all.append('\t'.join(name))
                    otu_file_temp = open(temp_otu_file, 'w')
                    otu_file_temp.writelines(new_all)
                    otu_file_temp.close()
                    options['otutable'] = temp_otu_file
                    options['phy_newick'] = temp_tree_file
                else:
                    newicktree = get_level_newicktree(self.option('otu_id'), level=self.option('level'),
                                                      tempdir=self.work_dir, return_file=False, bind_obj=self)
                    temp_tree_file = self.work_dir + '/temp.tree'
                    tempfile = open(temp_tree_file, 'w')
                    tempfile.write(newicktree)
                    tempfile.close()
                    otu_table = self.option('otu_file').path
                    temp_otu_file = self.option('otu_file').path + '.temp'
                    all_lines = open(otu_table, 'r').readlines()
                    new_all = []
                    new_all.append(all_lines[0])
                    for line in all_lines[1:]:  # OTU表中有复杂的名称OTU名称，包含进化物种类型，进化树种只有OTU名称
                        name = line.split('\t')
                        name[0] = name[0].split(';')[-1].strip()
                        new_all.append('\t'.join(name))
                    otu_file_temp = open(temp_otu_file, 'w')
                    otu_file_temp.writelines(new_all)
                    otu_file_temp.close()
                    options['otutable'] = temp_otu_file
                    options['phy_newick'] = temp_tree_file
        task.set_options(options)
        task.on('end', self.set_db)
        task.run()
        self.output_dir = task.output_dir
        super(BetaMultiAnalysisWorkflow, self).run()

    def set_db(self):
        """
        保存结果距离矩阵表到mongo数据库中
        """
        api_multi = self.api.beta_multi_analysis
        dir_path = self.output_dir
        cond, cons = [], []
        if self.option('env_file').is_set:
            cond, cons = self.classify_env(self.option('env_file').path)
            self.logger.info(cond)
            self.logger.info(cons)
        if not os.path.isdir(dir_path):
            raise Exception("找不到报告文件夹:{}".format(dir_path))

        api_multi.add_beta_multi_analysis_result(dir_path, self.option('analysis_type'),
                                                 main=False,
                                                 remove=cond,
                                                 main_id=str(self.option('main_id'))
                                                 )
        self.logger.info('运行self.end')
        self.end()

    def classify_env(self, env_file):
        """
        获取环境因子中哪些是条件（条件约束）型因子，哪些是数量（线性约束）型的因子
        """
        if isinstance(env_file, types.StringType) or isinstance(env_file, types.UnicodeType):
            if not os.path.exists(env_file):
                raise Exception('环境因子文件不存在')
        else:
            raise Exception('提供的环境因子文件名不是一个字符串')
        frame = np.loadtxt(env_file, dtype=str, comments='')
        len_env = len(frame[0])
        cond = []  # 记录不全是数字的因子
        cons = []  # 记录全是数字的因子
        for n in xrange(len_env - 1):
            env_values = frame[:, n + 1]
            for value in env_values[1:]:
                try:
                    float(value)
                except ValueError:
                    cond.append(env_values[0])
                    break
            else:
                cons.append(env_values[0])
        return cond, cons  # 前者不全是数字分组， 后者是全部都是数字的分组

    def end(self):
        repaths = [
            [".", "", "Beta_diversity分析结果文件目录"],
            ["Distance", "", "距离矩阵计算结果输出目录"],
            ["Dbrda", "", "db_rda分析结果目录"],
            ["Dbrda/db_rda_sites.xls", "xls", "db_rda样本坐标表"],
            ["Dbrda/db_rda_species.xls", "xls", "db_rda物种坐标表"],
            ["Dbrda/db_rda_centroids.xls", "xls", "db_rda哑变量环境因子坐标表"],
            ["Dbrda/db_rda_biplot.xls", "xls", "db_rda数量型环境因子坐标表"],
            ["Nmds", "", "NMDS分析结果输出目录"],
            ["Nmds/nmds_sites.xls", "xls", "样本坐标表"],
            ["Pca", "", "PCA分析结果输出目录"],
            ["Pca/pca_importance.xls", "xls", "主成分解释度表"],
            ["Pca/pca_rotation.xls", "xls", "物种主成分贡献度表"],
            ["Pca/pca_sites.xls", "xls", "样本坐标表"],
            ["Pca/pca_envfit_factor_scores.xls", "xls", "哑变量环境因子表"],
            ["Pca/pca_envfit_factor.xls", "xls", "哑变量环境因子坐标表"],
            ["Pca/pca_envfit_vector_scores.xls", "xls", "数量型环境因子表"],
            ["Pca/pca_envfit_vector.xls", "xls", "数量型环境因子坐标表"],
            ["Pcoa", "", "pcoa分析结果目录"],
            ["Pcoa/pcoa_eigenvalues.xls", "xls", "矩阵特征值"],
            ["Pcoa/pcoa_sites.xls", "xls", "样本坐标表"],
            ["Plsda", "", "plsda分析结果目录"],
            ["Plsda/plsda_sites.xls", "xls", "样本坐标表"],
            ["Plsda/plsda_rotation.xls", "xls", "物种主成分贡献度表"],
            ["Plsda/plsda_importance.xls", "xls", "主成分解释度表"],
            ["Rda", "", "rda_cca分析结果目录"],
            [r'Rda/dca.xls', 'xls', 'DCA分析结果'],
            ]
        regexps = [
            [r'Distance/%s.*\.xls$' % self.option('dist_method'), 'xls', '样本距离矩阵文件'],
            [r'Rda/.*_importance\.xls$', 'xls', '主成分解释度表'],
            [r'Rda/.*_sites\.xls$', 'xls', '样本坐标表'],
            [r'Rda/.*_species\.xls$', 'xls', '物种坐标表'],
            [r'Rda/.*_biplot\.xls$', 'xls', '数量型环境因子坐标表'],
            [r'Rda/.*_centroids\.xls$', 'xls', '哑变量环境因子坐标表'],
            ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        super(BetaMultiAnalysisWorkflow, self).end()
