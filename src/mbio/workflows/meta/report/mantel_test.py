# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
import os
from mbio.api.to_file.meta import *
import datetime
from mainapp.libs.param_pack import group_detail_sort


class MantelTestWorkflow(Workflow):
    """
    报告计算mantel检验
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(MantelTestWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", 'format': "meta.otu.otu_table"},  # 输入的OTU id
            {"name": "env_file", "type": "infile", 'format': "meta.otu.group_table"},  # 输入的环境因子表 id
            {"name": "newicktree", "type": "infile", 'format': "meta.otu.group_table"},  # 输入的环境因子表 id
            {"name": "level", "type": "int"},
            {"name": "otu_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "group_detail", "type": "string"},
            {"name": "partial_factor", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "env_method", "type": "string"},
            {"name": "species_method", "type": "string"},
            {"name": "partialmatrix", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "dis_matrix", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "fac_matrix", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"}
            ]
        self.add_option(options)
        # print(self._sheet.options())
        self.set_options(self._sheet.options())
        self.mantel = self.add_module('statistical.mantel_test')
        self.params = {}

    def run_mantel(self):
        options = {
            'otutable': self.option('otu_file'),
            'factor': self.option('env_file'),
            'level': self.option('level'),
            'partial_factor': self.option('partial_factor'),
            'otumatrixtype': self.option('species_method'),
            'factormatrixtype': self.option('env_method')
            }
        # print(self.option('indices'))
        self.mantel.set_options(options)
        self.mantel.on('end', self.set_db)
        self.mantel.run()
        self.output_dir = self.mantel.output_dir
        super(MantelTestWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        api_mantel = self.api.meta_species_env
        mantel_result = self.output_dir + "/mantel_results.txt"
        partial_matrix = self.option("partialmatrix")
        dis_matrix = self.option("dis_matrix")
        fac_matrix = self.option("fac_matrix")
        name = "mantel_test" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        if not os.path.isfile(mantel_result):
            raise Exception("找不到报告文件:{}".format(mantel_result))
        mantel_id = api_mantel.add_mantel_table(self.option('level'), self.option('otu_id'), self.option('env_id'), name=name, params=self.params)
        api_mantel.add_mantel_detail(mantel_result, mantel_id)
        api_mantel.add_mantel_matrix(partial_matrix, "partial_matrix", mantel_id)
        api_mantel.add_mantel_matrix(dis_matrix, "species_matrix", mantel_id)
        api_mantel.add_mantel_matrix(fac_matrix, "env_matrix", mantel_id)
        self.add_return_mongo_id('sg_species_mantel_check', mantel_id)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            # [".", "", "结果输出目录"],
            [".//mantel_results.txt", "txt", "mantel检验结果"]
        ])
        # print self.get_upload_files()
        super(MantelTestWorkflow, self).end()