# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
from mbio.api.to_file.meta import *
from mbio.packages.alpha_diversity.group_file_split import group_file_spilt


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
            {"name": "est_t_test_id", "type": "string"},
            {"name": "group_detail", "type": "string"}
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        # self.est_t_test = self.add_tool('statistical.metastat')
        self.tools = {}
        self.run_times = 0
        self.tools_num = 0
        self.group_file_dir = self.work_dir + '/two_group_output'

    def metastat_run(self, group_file, file_name):
        options = {
                'student_input': self.option('est_table'),
                'test': self.option('test_type'),
                'student_group': group_file
                }
        self.tools[file_name].set_options(options)
        # self.on_rely(self.tools[file_name], self.set_db, str(self.run_times))
        self.tools[file_name].on('end', self.set_db, str(self.run_times))
        self.tools[file_name].run()

    def run(self):
        file_path = group_file_spilt(self.option('group_table').prop['path'], self.group_file_dir)
        files = os.listdir(file_path)
        self.tools_num = len(files)
        for f in files:
            self.run_times += 1
            print(self.run_times)
            self.tools[f] = self.add_tool('statistical.metastat')
            # print(self.tools.values())
            group_file = os.path.join(file_path, f)
            print(group_file)
            self.metastat_run(group_file, f)
        super(EstTTestWorkflow, self).run()

    def set_db(self, event):
        """
        保存结果指数表到mongo数据库中
        """
        obj = event['bind_object']
        api_est_t_test = self.api.est_t_test
        est_t_path = obj.output_dir+"/student_result.xls"
        self.logger.info(est_t_path)
        # print(est_t_path)
        if not os.path.isfile(est_t_path):
            raise Exception("找不到报告文件:{}".format(est_t_path))
        api_est_t_test.add_est_t_test_detail(est_t_path, self.option('est_t_test_id'))
        # os.rename(est_t_path, self.output_dir+'/student_result{}.xls'.format(random.randint(1, 100)))
        if event['data'] == str(self.tools_num):
            self.end()