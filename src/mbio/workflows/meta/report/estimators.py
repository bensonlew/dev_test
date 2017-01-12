# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
import os
from mbio.api.to_file.meta import *
import datetime
from mainapp.libs.param_pack import group_detail_sort


class EstimatorsWorkflow(Workflow):
    """
    报告中计算alpha多样性指数时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(EstimatorsWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", 'format': "meta.otu.otu_table"},  # 输入的OTU id
            {"name": "otu_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "indices", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "est_id", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "group_detail", "type": "string"}
            ]
        self.add_option(options)
        # print(self._sheet.options())
        self.set_options(self._sheet.options())
        self.estimators = self.add_tool('meta.alpha_diversity.estimators')

    def run(self):
        options = {
            'otu_table': self.option('otu_file'),
            'indices': self.option('indices')
            }
        # print(self.option('indices'))
        self.estimators.set_options(options)
        self.estimators.on('end', self.set_db)
        self.estimators.run()
        self.output_dir = self.estimators.output_dir
        super(EstimatorsWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        api_estimators = self.api.estimator
        est_path = self.output_dir+"/estimators.xls"
        if not os.path.isfile(est_path):
            raise Exception("找不到报告文件:{}".format(est_path))
        est_id = api_estimators.add_est_table(est_path, level=self.option('level'),
                                              otu_id=self.option('otu_id'), est_id=self.option("est_id"))
        # self.add_return_mongo_id('sg_alpha_diversity', est_id)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            # [".", "", "结果输出目录"],
            ["./estimators.xls", "xls", "alpha多样性指数表"]
        ])
        # print self.get_upload_files()
        super(EstimatorsWorkflow, self).end()
