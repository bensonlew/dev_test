# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
from mbio.api.to_file.meta import *
from mbio.packages.alpha_diversity.group_file_split import group_file_spilt
from mainapp.libs.param_pack import group_detail_sort
import datetime


class EstTTestWorkflow(Workflow):
    """
    报告中计算alpha多样性指数时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(EstTTestWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "est_table", "type": "infile", 'format': "meta.otu.otu_table"},  # 输入的OTU id
            {"name": "group_table", "type": "infile", 'format': "meta.otu.group_table"},
            {"name": "otu_id", "type": "string"},
            {"name": "est_id", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "est_t_test_id", "type": "string"},
            {"name": "group_detail", "type": "string"},
            {"name": "group_detail", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "taskType", "type": "string"}
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.est_t_test = self.add_tool('statistical.metastat')
        # self.tools = {}
        self.group_file_dir = self.work_dir + '/two_group_output'

    # def check_options(self):
    #     otu_sample = self.option('est_table').get_sample_info()
    #     # self.logger.info(otu_sample)
    #     group_sample, header, is_empty = self.option('group_table').get_file_info()
    #     for name in group_sample:
    #         if name not in otu_sample:
    #             raise Exception('分组样本不在otu表所拥有的样本内，请检查分组方案')

    def run(self):
        file_path = group_file_spilt(self.option('group_table').prop['path'], self.group_file_dir)
        # print(file_path)
        options = {
                'est_input': self.option('est_table'),
                'test': 'estimator',
                'est_group': file_path
                }
        self.est_t_test.set_options(options)
        self.est_t_test.on('end', self.set_db)
        self.est_t_test.run()
        self.output_dir = self.est_t_test.output_dir
        super(EstTTestWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        api_est_t_test = self.api.est_t_test
        if not os.path.isdir(self.output_dir):
            raise Exception("找不到报告文件夹:{}".format(self.output_dir))
        my_param = dict()
        my_param['alpha_diversity_id'] = self.option("est_id")
        my_param['group_detail'] = group_detail_sort(self.option("group_detail"))
        my_param['group_id'] = self.option("group_id")
        my_param['submit_location'] = self.option("submit_location")
        my_param['taskType'] = self.option("taskType")
        params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        name = "est_t_test_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        est_t_test_id = api_est_t_test.add_est_t_test_collection(params, self.option("group_id"), self.option("est_id"), name)
        for f in os.listdir(self.output_dir):
            self.logger.info(os.path.join(self.output_dir, f))
            api_est_t_test.add_est_t_test_detail(os.path.join(self.output_dir, f), est_t_test_id)
        self.add_return_mongo_id('sg_alpha_est_t_test', est_t_test_id)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [r".*\.xls", "", "alpha多样性指数T检验结果表"]
        ])
        # print self.get_upload_files()
        super(EstTTestWorkflow, self).end()
