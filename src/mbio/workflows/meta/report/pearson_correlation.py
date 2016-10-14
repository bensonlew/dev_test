# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
import glob
import os
from mbio.api.to_file.meta import *
import datetime
from mbio.packages.statistical.reverse_table import reverse_table
from mainapp.libs.param_pack import group_detail_sort


class PearsonCorrelationWorkflow(Workflow):
    """
    报告中计算alpha多样性指数时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(PearsonCorrelationWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", 'format': "meta.otu.otu_table"},  # 输入的OTU id
            {"name": "otu_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "params", "type": "string"},
            {"name": "env_file", "type": "infile", 'format': "meta.otu.group_table"},  # 输入的OTU id
            {"name": "env_id", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "correlation_id", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "group_detail", "type": "string"}
            ]
        self.add_option(options)
        # print(self._sheet.options())
        self.set_options(self._sheet.options())
        self.correlation = self.add_tool('statistical.pearsons_correlation')
        self.distance_otu = self.add_tool('meta.beta_diversity.distance_calc')
        self.distance_env = self.add_tool('meta.beta_diversity.distance_calc')
        self.hcluster_otu = self.add_tool('meta.beta_diversity.hcluster')
        self.hcluster_env = self.add_tool('meta.beta_diversity.hcluster')
        self.distance = [self.distance_otu, self.distance_env]
        self.hcluster = [self.hcluster_otu, self.hcluster_env]
        self.tools = [self.correlation] + self.distance + self.hcluster
        self.params = {}

    def run_correlation(self):
        options = {
            'otutable': self.option('otu_file'),
            'envtable': self.option('env_file')
            }
        self.correlation.set_options(options)
        self.correlation.run()
        # self.output_dir = self.correlation.output_dir
        # super(PearsonCorrelationWorkflow, self).run()

    def run_distance(self):
        # print(self.option("otu_file"))
        otu_file = self.work_dir + "/reverse_otu.xls"
        print(self.work_dir + "/otu_file.xls")
        print(self.option("env_file").path)
        reverse_table(self.work_dir + "/otu_file.xls", otu_file)
        print(otu_file)
        self.distance_otu.set_options({
            "otutable": otu_file
        })
        self.distance_env.set_options({
            "otutable": self.option("env_file").path
        })
        # self.distance_env.on('end', self.run_hcluster)
        self.on_rely(self.distance, self.run_hcluster)
        self.distance_env.run()
        self.distance_otu.run()

    def run_hcluster(self):
        distance_env = glob.glob(self.distance_env.output_dir + "/*")[0]
        distance_otu = glob.glob(self.distance_otu.output_dir + "/*")[0]
        self.hcluster_env.set_options({
            "dis_matrix": distance_env
        })
        self.hcluster_otu.set_options({
            "dis_matrix": distance_otu
        })
        self.hcluster_env.run()
        self.hcluster_otu.run()

    def run(self):
        self.run_correlation()
        self.run_distance()
        self.on_rely(self.tools, self.set_db)
        super(PearsonCorrelationWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        self.params = eval(self.option("params"))
        del self.params["otu_file"]
        del self.params["env_file"]
        group_detail = self.params["group_detail"]
        self.params["group_detail"] = group_detail_sort(group_detail)
        species_tree = ""
        env_tree = ""
        api_correlation = self.api.meta_species_env
        corr_path = glob.glob(self.correlation.output_dir+"/*correlation*")
        pvalue_path = glob.glob(self.correlation.output_dir+"/*pvalue*")
        env_tree_path = self.hcluster_env.output_dir + "/hcluster.tre"
        species_tree_path = self.hcluster_otu.output_dir + "/hcluster.tre"
        if os.path.exists(env_tree_path):
            with open(env_tree_path, "r") as f:
                env_tree = f.readline().strip()
        if os.path.exists(species_tree_path):
            with open(species_tree_path, "r") as f:
                species_tree = f.readline().strip()
        name = "correlation" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        corr_id = api_correlation.add_correlation(self.option("level"), self.option("otu_id"), self.option("env_id"), species_tree=species_tree, env_tree=env_tree, name=name, params=self.params)
        api_correlation.add_correlation_detail(corr_path[0], "correlation", corr_id)
        api_correlation.add_correlation_detail(pvalue_path[0], "pvlue", corr_id)
        self.add_return_mongo_id('sg_species_env_correlation', corr_id)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.correlation.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
            # ["./mantel_results.txt", "txt", "mantel检验结果"]
        ])
        # print self.get_upload_files()
        super(PearsonCorrelationWorkflow, self).end()
