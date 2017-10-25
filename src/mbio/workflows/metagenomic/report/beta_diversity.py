# -*- coding: utf-8 -*-
# __author__ = 'zouxuan'

"""beta多样性分析"""

import os
import re
import numpy as np
import types
from biocluster.workflow import Workflow


class BetaDiversityWorkflow(Workflow):
    """
    交互分析beta多样性
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(BetaDiversityWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "analysis_type", "type": "string", "default": 'pca'},
            {"name": "anno_type", "type": "string", "default": 'nr'},
            {"name": "anno_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "main_id", "type": "string"},
            {"name": "anno_table", "type": "infile", "format": "meta.profile"},  # 各数据库的注释表格
            {"name": "geneset_id", "type": "string"},
            {"name": "geneset_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "profile_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "method", "type": "string", "default": "rpkm"},
            {"name": "distance_method", "type": "string", "default": "bray_curtis"},
            {"name": "level_id", "type": "string"},
            {"name": "second_level", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "params", "type": "string"},
            {"name": "group_id", "type": "string", "default": ""},
            {"name": "env_file", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "group_detail", "type": "string", "default": ""},
            {"name": "group_table", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "env_labs", "type": "string", "default": ""},
            {"name": "group_id", "type": "string", "default": ""},
            {"name": "env_id", "type": "string", "default": ""},
            {"name": "gene_list", "type": "infile", "format": "meta.profile"},
            {"name": "lowest_level", "type": "string", "default": ""}  # 注释表数据库对应的最低分类，eg：KEGG的ko
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.abundance = self.add_tool("meta.create_abund_table")
        self.beta = self.add_module("meta.beta_diversity.beta_diversity")
        self.sam = self.add_tool("meta.otu.sort_samples")

    def run(self):
        #self.IMPORT_REPORT_DATA = True
        #self.IMPORT_REPORT_DATA_AFTER_END = False
        if self.option("group_table").is_set:
            if self.option("profile_table").is_set:
                self.sam.on('end', self.run_beta)
            else:
                self.abundance.on('end', self.sort_sample)
                self.sam.on('end', self.run_beta)
        else:
            if self.option("profile_table").is_set:
                pass
            else:
                self.abundance.on('end', self.run_beta)
        if self.option("profile_table").is_set:
            if self.option("group_table").is_set:
                self.sort_sample()
            else:
                self.run_beta()
        else:
                self.run_abundance()
        super(BetaDiversityWorkflow, self).run()

    def run_abundance(self):
        options = {
            'anno_table': self.option('anno_table'),
            'geneset_table': self.option('geneset_table'),
            'level_type': self.option('level_id'),
            'level_type_name': self.option('second_level'),
            'gene_list': self.option('gene_list'),
            'lowest_level': self.option('lowest_level')
        }
        self.abundance.set_options(options)
        self.abundance.run()

    def sort_sample(self):
        if self.option("profile_table").is_set:
            otutable = self.option("profile_table")
        else:
            otutable = self.abundance.option('out_table')
        options = {
            'in_otu_table': otutable,
            'group_table': self.option("group_table")
        }
        self.sam.set_options(options)
        self.sam.run()

    def run_beta(self):
        if self.option("group_table").is_set:
            otutable = self.sam.option("out_otu_table")
        else:
            if self.option("profile_table").is_set:
                otutable = self.option("profile_table")
            else:
                otutable = self.abundance.option('out_table')
        options = {
            'analysis': self.option('analysis_type'),
            # 'dis_method': self.option('dist_method'),
            'otutable': otutable

        }
        if self.option('env_file').is_set:
            options['envlabs'] = self.option('env_labs')
            options['envtable'] = self.option('env_file')
        else:
            pass
        if self.option('analysis_type') in ['pcoa', 'nmds', 'dbrda']:
            options['dis_method'] = self.option('distance_method')
        self.beta.set_options(options)
        self.beta.on('end', self.set_db)
        self.beta.run()
        self.output_dir = self.beta.output_dir

    def set_db(self):
        """
        保存结果距离矩阵表到mongo数据库中
        """
        api_beta = self.api.api('metagenomic.beta_diversity')
        dir_path = self.output_dir
        cond, cons = [], []
        if self.option('env_file').is_set:
            cond, cons = self.classify_env(self.option('env_file').path)
            self.logger.info(cond)
            self.logger.info(cons)
        if not os.path.isdir(dir_path):
            raise Exception("找不到报告文件夹:{}".format(dir_path))
        api_beta.add_beta_diversity(dir_path, self.option('analysis_type'),
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
        if self.option('analysis_type') == 'plsda':  # add 14 lines by hongdongxuan 20170327
            file_name = "PLS_DA分析结果目录"
        elif self.option('analysis_type') == 'pca':
            file_name = "PCA分析结果目录"
        elif self.option('analysis_type') == 'pcoa':
            file_name = "PCoA分析结果目录"
        elif self.option('analysis_type') == 'nmds':
            file_name = "NMDS分析结果目录"
        elif self.option('analysis_type') == 'dbrda':
            file_name = "db-RDA分析结果目录"
        elif self.option('analysis_type') == 'rda_cca':
            file_name = "RDA_CCA分析结果目录"
        else:
            file_name = "Beta_diversity分析结果目录"
        repaths = [
            [".", "", file_name],
            ["Distance", "", "距离矩阵计算结果输出目录"],
            ["Dbrda", "", "db-RDA分析结果目录"],
            ["Dbrda/db_rda_sites.xls", "xls", "db_rda样本坐标表"],
            ["Dbrda/db_rda_species.xls", "xls", "db_rda物种坐标表"],
            ["Dbrda/db_rda_centroids.xls", "xls", "db_rda哑变量环境因子坐标表"],
            ["Dbrda/db_rda_biplot.xls", "xls", "db_rda数量型环境因子坐标表"],
            ["Nmds", "", "NMDS分析结果输出目录"],
            ["Nmds/nmds_sites.xls", "xls", "样本坐标表"],
            ["Nmds/nmds_stress.xls", "xls", "样本特征拟合度值"],
            ["Pca", "", "PCA分析结果输出目录"],
            ["Pca/pca_importance.xls", "xls", "主成分解释度表"],
            ["Pca/pca_rotation.xls", "xls", "物种主成分贡献度表"],
            ["Pca/pca_sites.xls", "xls", "样本坐标表"],
            ["Pca/pca_envfit_factor_scores.xls", "xls", "哑变量环境因子表"],
            ["Pca/pca_envfit_factor.xls", "xls", "哑变量环境因子坐标表"],
            ["Pca/pca_envfit_vector_scores.xls", "xls", "数量型环境因子表"],
            ["Pca/pca_envfit_vector.xls", "xls", "数量型环境因子坐标表"],
            ["Pcoa", "", "PCoA分析结果目录"],
            ["Pcoa/pcoa_eigenvalues.xls", "xls", "矩阵特征值"],
            ["Pcoa/pcoa_eigenvaluespre.xls", "xls", "特征解释度百分比"],
            ["Pcoa/pcoa_sites.xls", "xls", "样本坐标表"],
            ["Plsda", "", "PLS_DA分析结果目录"],
            ["Plsda/plsda_sites.xls", "xls", "样本坐标表"],
            ["Plsda/plsda_rotation.xls", "xls", "物种主成分贡献度表"],
            ["Plsda/plsda_importance.xls", "xls", "主成分组别特征值表"],
            ["Plsda/plsda_importancepre.xls", "xls", "主成分解释度表"],
            ["Rda", "", "RDA_CCA分析结果目录"],
            [r'Rda/dca.xls', 'xls', 'DCA分析结果'],
        ]
        regexps = [
            [r'Distance/%s.*\.xls$' % self.option('distance_method'), 'xls', '样本距离矩阵文件'],
            [r'Rda/.*_importance\.xls$', 'xls', '主成分解释度表'],
            [r'Rda/.*_sites\.xls$', 'xls', '样本坐标表'],
            [r'Rda/.*_species\.xls$', 'xls', '物种坐标表'],
            [r'Rda/.*_biplot\.xls$', 'xls', '数量型环境因子坐标表'],
            [r'Rda/.*_centroids\.xls$', 'xls', '哑变量环境因子坐标表'],
            [r'Rda/.*_envfit\.xls$', 'xls', 'p_value值与r值表'],
        ]
        sdir = self.add_upload_dir(self.output_dir)
        sdir.add_relpath_rules(repaths)
        sdir.add_regexp_rules(regexps)
        super(BetaDiversityWorkflow, self).end()
