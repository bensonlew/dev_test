# -*- coding: utf-8 -*-
# __author__ = 'xuting'

"""群落组成分析模块"""
import os
import json
import datetime
from biocluster.core.exceptions import OptionError
from biocluster.workflow import Workflow
from mainapp.models.mongo.public.meta.meta import Meta


class ClusterAnalysisWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(ClusterAnalysisWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_otu_table", "type": "infile", "format": "meta.otu.otu_table"},  # 输入的OTU表
            {"name": "input_otu_id", "type": "string"},  # 输入的OTU id
            {"name": "level", "type": "string", "default": "9"},  # 输入的OTU level
            {"name": "group_detail", "type": "string"},  # 输入的group_detail 示例如下
            # {"A":["578da2fba4e1af34596b04ce","578da2fba4e1af34596b04cf","578da2fba4e1af34596b04d0"],"B":["578da2fba4e1af34596b04d1","578da2fba4e1af34596b04d3","578da2fba4e1af34596b04d5"],"C":["578da2fba4e1af34596b04d2","578da2fba4e1af34596b04d4","578da2fba4e1af34596b04d6"]}
            {"name": "method", "type": "string", "default": ""}  # 聚类方式， ""为不进行聚类
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.sort_samples = self.add_tool("meta.otu.sort_samples")
        self.matrix = self.add_tool("meta.beta_diversity.distance_calc")
        self.hcluster = self.add_tool('meta.beta_diversity.hcluster')
        group_table_path = os.path.join(self.work_dir, "group_table.xls")
        self.group_table_path = Meta().group_detail_to_table(self.option("group_detail"), group_table_path)

    def check_options(self):
        if self.option('method') not in ['average', 'single', 'complete', ""]:
            raise OptionError('错误的层级聚类方式：%s' % self.option('method'))

    def run_sort_samples(self):
        self.sort_samples.set_options({
            "in_otu_table": self.option("in_otu_table"),
            "group_table": self.group_table_path
        })
        if self.option("method") != "":
            self.sort_samples.on("end", self.run_matrix)
        else:
            self.sort_samples.on("end", self.set_db)
        self.sort_samples.run()

    def run_matrix(self):
        trans_otu = os.path.join(self.work_dir, "otu.trans")
        self.sort_samples.option("out_otu_table").transposition(trans_otu)
        self.matrix.set_options({
            "method": "bray_curtis",
            "otutable": trans_otu
        })
        self.matrix.on('end', self.run_cluster)
        self.matrix.run()

    def run_cluster(self):
        options = {
            "dis_matrix": self.matrix.option('dis_matrix'),
            "linkage": self.option("method")
        }
        self.hcluster.set_options(options)
        self.hcluster.on('end', self.set_db)
        self.hcluster.run()

    def set_db(self):
        self.logger.info("正在写入mongo数据库")
        newick_id = ""
        myParams = json.loads(self.sheet.params)
        if self.option("method") != "":
            api_heat_cluster = self.api.heat_cluster
            name = "heat_cluster_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            newick_id = api_heat_cluster.create_newick_table(self.sheet.params, self.option("method"), myParams["otu_id"], name)
            self.hcluster.option("newicktree").get_info()
            api_heat_cluster.update_newick(self.hcluster.option("newicktree").prop['path'], newick_id)
            self.add_return_mongo_id("sg_newick_tree", newick_id, "", False)
        api_otu = self.api.cluster_analysis
        new_otu_id = api_otu.add_sg_otu(self.sheet.params, self.option("input_otu_id"), None, newick_id)
        api_otu.add_sg_otu_detail(self.sort_samples.option("out_otu_table").prop["path"], new_otu_id, self.option("input_otu_id"))
        self.add_return_mongo_id("sg_otu", new_otu_id)
        self.end()

    def run(self):
        self.run_sort_samples()
        super(ClusterAnalysisWorkflow, self).run()
