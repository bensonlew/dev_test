# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

"""otu表的样本距离计算"""

from biocluster.workflow import Workflow
import os


class DistanceCalcWorkflow(Workflow):
    """
    报告中调用otu计算样本距离时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(DistanceCalcWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "sting"},  # 需要修改，紧紧作为测试
            {"name": "name", "type": "string"},  # 需要修改，紧紧作为测试
            {"name": "method", "type": "string", "default": 'bray_curtis'},
            {"name": "update_info", "type": "string"},
            {"name": "otu_id", "type": "string"},
            {"name": "category_name", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "matrix_id", "type": "string"},
            # {"name": "matrix_out", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def run(self):
        task = self.add_tool("meta.beta_diversity.distance_calc")
        if self.UPDATE_STATUS_API:
            task.UPDATE_STATUS_API = self.UPDATE_STATUS_API
        self.logger.info(self._sheet.options('otu_file'))
        if 'unifrac' in self._sheet.options('method'):
            newicktree = self.get_phylo_tree()
            options = {
                'method': self._sheet.options('method'),
                'otutable': self._sheet.options('otu_file'),
                'newicktree': newicktree
            }
        else:
            options = {
                'method': self.options('method'),
                'otutable': '/mnt/ilustre/users/sanger/sheng.he_test/test_file/my_otu_table.txt'
            }
        task.set_options(options)
        task.on('end', self.set_db)
        task.run()
        self.output_dir = task.output_dir
        super(DistanceCalcWorkflow, self).run()

    def get_phylo_tree(self):
        tree_path = ''
        return tree_path

    def set_db(self):
        """
        保存结果距离矩阵表到mongo数据库中
        """
        api_distance = self.api.distance
        matrix_path = self.output_dir + '/' + os.listdir(self.output_dir)[0]
        if not os.path.isfile(matrix_path):
            raise Exception("找不到报告文件:{}".format(matrix_path))
        api_distance.add_distance_table(matrix_path, 9, )
        self.end()
