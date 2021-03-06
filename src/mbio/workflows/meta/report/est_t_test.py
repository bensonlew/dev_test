# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
from mbio.packages.alpha_diversity.group_file_split import group_file_spilt
import os
from biocluster.core.exceptions import OptionError


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
            {"name": "est_test_method", "type": "string", "default": "student"},
            {"name": "submit_location", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "task_type", "type": "string"}
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.est_t_test = self.add_tool('statistical.metastat')
        self.group_name = ''
        self.group_file_dir = self.work_dir + '/two_group_output'

    def check_options(self):
        with open(self.option("est_table").prop['path'], "r") as f:
            firstline = f.readline()
            bad_line_num = 0
            total_line = 0
            for line in f:
                total_line += 1
                line = line.strip().split("\t")[1:]
                line_set = set(line)
                if len(line_set) == 1:
                    bad_line_num += 1
            if total_line == bad_line_num:
                raise OptionError("输入的多样性指数表格格式不正确，全部样本指数值相同")
            print bad_line_num, total_line

    def run(self):
        group_name = group_file_spilt(self.option('group_table').prop['path'], self.group_file_dir)
        name_list = []
        for g in group_name:
            if g[0] > g[1]:
                gg = g[1]+'|'+g[0]
                name_list.append(gg)
            else:
                gg = g[0]+'|'+g[1]
                name_list.append(gg)
        self.group_name = ",".join(name_list)
        self.logger.info(self.group_name)
        self.logger.info(self.option('est_test_method'))
        options = {
                'est_input': self.option('est_table'),
                'test': 'estimator',
                'est_group': self.group_file_dir,
                'est_test_method': self.option('est_test_method')
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
        # print(self.option("otu_id"))
        # my_param = dict()
        # my_param['alpha_diversity_id'] = self.option("est_id")
        # my_param['group_detail'] = group_detail_sort(self.option("group_detail"))
        # my_param['group_id'] = self.option("group_id")
        # my_param['submit_location'] = self.option("submit_location")
        # my_param['task_type'] = self.option("task_type")
        # my_param['otu_id'] = self.option("otu_id")
        # my_param['test_method'] = self.option("est_test_method")
        # params = json.dumps(my_param, sort_keys=True, separators=(',', ':'))
        # print(params)
        # name = "est_t_test_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
        # est_t_test_id = api_est_t_test.add_est_t_test_collection(params, self.option("group_id"), self.option("est_id"), name=name, group_name=self.group_name)
        est_t_test_id = self.option("est_t_test_id")
        for f in os.listdir(self.output_dir):
            self.logger.info(os.path.join(self.output_dir, f))
            api_est_t_test.add_est_t_test_detail(os.path.join(self.output_dir, f), est_t_test_id, group_name=self.group_name)
        # self.add_return_mongo_id('sg_alpha_ttest', est_t_test_id)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "多样性指数结果目录"]
        ])
        result_dir.add_regexp_rules([
            [r".*\.xls", "", "alpha多样性指数T检验结果表"]
        ])
        # print self.get_upload_files()
        super(EstTTestWorkflow, self).end()
