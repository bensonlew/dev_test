# -*- coding: utf-8 -*-
# __author__ = 'zouxuan'

"""距离矩阵层级聚类"""

import datetime
from biocluster.workflow import Workflow
from bson import ObjectId
import re
import os
import json
import shutil

class HclusterWorkflow(Workflow):
    """
    报告中调用距离矩阵计算样本层级聚类数使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(HclusterWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "anno_id", "type": "string"},
            {"name": "anno_table", "type": "infile", "format": "meta.profile"},  # 各数据库的注释表格
            {"name": "geneset_id", "type": "string"},
            {"name": "geneset_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "otu_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "distance_method", "type": "string", "default": "bray_curtis"},
            {"name": "hcluster_method", "type": "string", "default": "average"},
            {"name": "level_id", "type": "string"},
            {"name": "second_level", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "params", "type": "string"},
            {"name": "main_id", "type": "string"},
            {"name": "group_detail", "type": "string"},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.dist = self.add_tool("meta.beta_diversity.distance_calc")
        self.hcluster = self.add_tool("meta.beta_diversity.hcluster")
        self.abundance = self.add_tool("meta.create_abund_table")

    def run(self):
        self.IMPORT_REPORT_DATA = True
        self.IMPORT_REPORT_DATA_AFTER_END = False
        self.abundance.on('end', self.run_dist)
        self.dist.on('end', self.run_hcluster)
        self.hcluster.on('end', self.set_db)
        if self.option("otu_table").is_set:
            self.run_dist()
        else:
            self.run_abundance()
        super(HclusterWorkflow, self).run()

    def run_dist(self):
        if self.option("otu_table").is_set:
            otutable = self.option("otu_table")
        else:
            otutable = self.abundance.option('out_table')
        options = {
            'method': self.option('distance_method'),
            'otutable': otutable
        }
        self.dist.set_options(options)
        self.dist.run()

    def run_hcluster(self):
        options = {
            'linkage': self.option('hcluster_method'),
            'dis_matrix': self.dist.option('dis_matrix')
        }
        self.hcluster.set_options(options)
        self.hcluster.run()

    def run_abundance(self):
        options = {
            'anno_table': self.option('anno_table'),
            'geneset_table': self.option('geneset_table'),
            'level_type': self.option('level_id'),
            'level_type_name': self.option('second_level')
        }
        self.abundance.set_options(options)
        self.abundance.run()

    def set_db(self):
        params_json = json.loads(self.option('params'))
        matrix_path = self.dist.output_dir + '/' + os.listdir(self.dist.output_dir)[0]
        final_matrix_path = os.path.join(self.output_dir, os.listdir(self.dist.output_dir)[0])
        shutil.copy2(matrix_path, final_matrix_path)
        if not os.path.isfile(matrix_path):
            raise Exception("找不到报告文件:{}".format(matrix_path))
        self.api_dist = self.api.api('metagenomic.distance_metagenomic')
        dist_id = self.api_dist.add_dist_table(matrix_path,main=True, level_id=self.option('level_id'),
                                               anno_id=self.option('anno_id'), name=None,
                                               params=params_json, geneset_id=self.option('geneset_id'))
        newick_fath = self.hcluster.output_dir + "/hcluster.tre"
        final_newick_path = os.path.join(self.output_dir, "hcluster.tre")
        shutil.copy2(newick_fath, final_newick_path)
        if not os.path.isfile(newick_fath):
            raise Exception("找不到报告文件:{}".format(newick_fath))
        self.api_tree = self.api.api('metagenomic.hcluster_tree')
        self.api_tree.add_hcluster_tree(newick_fath, main=False, tree_id=self.option('main_id'),
                                        update_dist_id=dist_id)
        self.end()

    def end(self):
        result_dir_hucluster = self.add_upload_dir(self.output_dir)
        result_dir_hucluster.add_relpath_rules([
            [".", "", "样本层级聚类分析结果目录"],
            ["./hcluster.tre", "tre", "层级聚类树结果表"]  # modified by hongdongxuan 20170321
        ])
        result_dir_hucluster.add_regexp_rules([
            [r'%s.*\.xls' % self.option('distance_method'), 'xls', '样本距离矩阵文件']
        ])
        super(HclusterWorkflow, self).end()
