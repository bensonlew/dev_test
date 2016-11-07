# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
import glob
import os
from mbio.api.to_file.meta import *
import datetime
from mbio.packages.statistical.reverse_table import reverse_table
from mainapp.libs.param_pack import group_detail_sort
import re


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
            {"name": "env_labs", "type": "string"},
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
        self.params = {}

    def run_correlation(self):
        options = {
            'otutable': self.option('otu_file'),
            'envtable': self.option('env_file')
            }
        self.correlation.set_options(options)
        self.correlation.on("end", self.set_db)
        self.correlation.run()
        # self.output_dir = self.correlation.output_dir
        # super(PearsonCorrelationWorkflow, self).run()
        
    def run(self):
        self.run_correlation()
        # self.run_distance()
        # self.on_rely(self.tools, self.set_db)
        super(PearsonCorrelationWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        self.params = eval(self.option("params"))
        del self.params["otu_file"]
        del self.params["env_file"]
        level = self.params["level"]
        del self.params["level"]
        self.params["level_id"] = int(level)
        group_detail = self.params["group_detail"]
        self.params["group_detail"] = group_detail_sort(group_detail)
        species_tree = ""
        env_tree = ""
        env_list = []
        species_list = []
        api_correlation = self.api.meta_species_env
        corr_path = glob.glob(self.correlation.output_dir+"/*correlation*")
        pvalue_path = glob.glob(self.correlation.output_dir+"/*pvalue*")

        env_tree_path = self.correlation.work_dir + "/env_tree.tre"
        species_tree_path = self.correlation.work_dir + "/species_tree.tre"

        if os.path.exists(env_tree_path):
            with open(env_tree_path, "r") as f:
                env_tree = f.readline().strip()
                raw_samp = re.findall(r'([(,]([\[\]\.\;\'\"\ 0-9a-zA-Z_-]+?):[0-9])', env_tree)
                env_list = [i[1] for i in raw_samp]
                # env_list = sorted(env_list)
                print("llllllllll")
                print(env_list)
        if os.path.exists(species_tree_path):
            with open(species_tree_path, "r") as f:
                species_tree = f.readline().strip()
                raw_samp = re.findall(r'([(,]([\[\]\.\;\'\"\ 0-9a-zA-Z_-]+?):[0-9])', species_tree)
                species_list = [i[1] for i in raw_samp]
                # species_list = sorted(species_list)
                # print(species_list)
                print("llllllllll")
        name = "correlation" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        corr_id = api_correlation.add_correlation(self.option("level"), self.option("otu_id"), self.option("env_id"),
                                                  species_tree=species_tree, env_tree=env_tree, name=name,
                                                  params=self.params, env_list=env_list, species_list=species_list)
        api_correlation.add_correlation_detail(corr_path[0], "correlation", corr_id)
        api_correlation.add_correlation_detail(pvalue_path[0], "pvalue", corr_id)
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
