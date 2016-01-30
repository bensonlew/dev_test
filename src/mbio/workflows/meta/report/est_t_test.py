# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
# import os
from mbio.api.to_file.meta import *


class EstTTestWorkflow(Workflow):
    """
    报告中计算alpha多样性指数时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(EstTTestWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "est_table", "type": "infile", 'format': "meta.otu.otu_table"},  # 输入的OTU id
            {"name": "group_table", "type": "infile", 'format': "meta.otu.group_table"},
            {"name": "update_info", "type": "string"},
            {"name": "test_type", "type": "string"},
            # {"name": "est_t_test_id", "type": "string"}
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.est_t_test = self.add_tool('statistical.metastat')

    def run(self):
        super(EstTTestWorkflow, self).run()
        # if self.UPDATE_STATUS_API:
        #     self.est_t_test.UPDATE_STATUS_API = self.UPDATE_STATUS_API
        self.est_t_test.set_options({
            'student_input': self.option('est_table'),
            'test': self.option('test_type'),
            'student_group': self.option('group_table')
            })
        self.est_t_test.on('end', self.set_db)
        self.est_t_test.run()
        self.output_dir = self.est_t_test.output_dir

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        api_est_t_test = self.api.stat_test
        est_t_path = self.output_dir+"/student_result.xls"
        if not os.path.isfile(est_t_path):
            raise Exception("找不到报告文件:{}".format(est_t_path))
        api_est_t_test.add_twosample_species_difference_check(est_t_path, self.option('est_id'))
        self.end()
