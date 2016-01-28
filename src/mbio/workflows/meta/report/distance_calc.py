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
            {"name": "otu_file", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "method", "type": "string", "default": 'bray_curtis'},
            {"name": "update_info", "type": "string"},
            {"name": "otu_id", "type": "string"},
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
        self.logger.info(self._sheet.option('otu_file'))
        if 'unifrac' in self._sheet.option('method'):
            newicktree = self.get_phylo_tree()
            options = {
                'method': self._sheet.option('method'),
                'otutable': self._sheet.option('otu_file'),
                'newicktree': newicktree
            }
        else:
            options = {
                'method': self.option('method'),
                'otutable': self.option('otu_file')
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
        api_distance.add_dist_table(matrix_path, dist_id=self.option('matrix_id'), )
        self.logger.info('运行self.end')
        self.end()
