# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
# import os
from mbio.api.to_file.meta import *


class EstimatorsWorkflow(Workflow):
    """
    报告中计算alpha多样性指数时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(EstimatorsWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_table", "type": "infile", 'format': "meta.otu.otu_table"},  # 输入的OTU id
            {"name": "otu_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "indices", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "est_id", "type": "string"}
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.estimators = self.add_tool('meta.alpha_diversity.estimators')

    def run(self):
        # super(EstimatorsWorkflow, self).run()
        # if self.UPDATE_STATUS_API:
        #     self.estimators.UPDATE_STATUS_API = self.UPDATE_STATUS_API
        options = {
            'otu_table': self.option('otu_table'),
            'indices': self.option('indices')
            }
        print(self.option('indices'))
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
        api_estimators.add_est_table(est_path, self.option('level'), est_id=self.option('est_id'))
        self.end()
