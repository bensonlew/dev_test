# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""组间差异性两组比较检验分析"""

from biocluster.workflow import Workflow
import os


class TwoGroupWorkflow(Workflow):
    """
    报告中调用组间差异性分析检验时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(TwoGroupWorkflow, self).__init__(wsheet_object)

    def run(self):
        task = self.add_tool("statistical.metastat.py")
        if self.UPDATE_STATUS_API:
            task.UPDATE_STATUS_API = self.UPDATE_STATUS_API
        if self._sheet.options('test') == 'student':
            options = {
                'test': self._sheet.options('test'),
                'student_group': self._sheet.options('student_category_name'),
                'student_input': self._sheet.options('otu_file'),
                'student_correction': self._sheet.options('student_correction'),
                'student_ci': self._sheet.options('student_ci'),
                'student_type': self._sheet.options('student_type')
            }
        elif self._sheet.options('test') == 'welch':
            options = {
                'test': self._sheet.options('test'),
                'welch_group': self._sheet.options('welch_category_name'),
                'welch_input': self._sheet.options('otu_file'),
                'welch_correction': self._sheet.options('welch_correction'),
                'welch_type': self._sheet.options('welch_type'),
                'welch_ci': self._sheet.options('welch_ci')
            }
        elif self._sheet.options('test') == 'mann':
            options = {
                'test': self._sheet.options('test'),
                'mann_group': self._sheet.options('mann_category_name'),
                'mann_input': self._sheet.options('otu_file'),
                'mann_correction': self._sheet.options('mann_correction'),
                'mann_type': self._sheet.options('mann_type'),
                'mann_ci': self._sheet.options('mann_ci')
            }
        task.set_options(options)
        task.on('end', self.set_db)
        task.run()
        self.output_dir = task.output_dir
        super(TwoGroupWorkflow, self).run()

    def set_db(self):
        """
        保存两组比较分析的结果表保存到mongo数据库中
        """
        api_two_group = self.api.database.two_group
        if self._sheet.options('test') == 'student':
            box_path = self.output_dir + '/student_boxfile.xls'
            two_group_path = self.output_dir + '/student_result.xls'
        elif self._sheet.options('test') == 'welch':
            box_path = self.output_dir + '/welch_boxfile.xls'
            two_group_path = self.output_dir + '/welch_result.xls'
        else:
            box_path = self.output_dir + '/mann_boxfile.xls'
            two_group_path = self.output_dir + '/mann_result.xls'
        if not os.path.isfile(box_path):
            raise Exception("找不到报告文件:{}".format(box_path))
        if not os.path.isfile(two_group_path):
            raise Exception("找不到报告文件:{}".format(two_group_path))
        #api_otu.add_otu_table(otu_path, 9, )
        self.end()