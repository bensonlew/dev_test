# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

"""距离矩阵层级聚类"""

from biocluster.workflow import Workflow
import os


class HclusterWorkflow(Workflow):
    """
    报告中调用距离矩阵计算样本层级聚类数使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(HclusterWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "distance_matrix", "type": "infile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "method", "type": "string", "default": 'average'},
            {"name": "update_info", "type": "string"},
            {"name": "distance_id", "type": "string"},
            {"name": "newick_id", "type": "string"},
            # {"name": "matrix_out", "type": "outfile", "format": "meta.beta_diversity.distance_matrix"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def run(self):
        task = self.add_tool("meta.beta_diversity.hcluster")
        if self.UPDATE_STATUS_API:
            task.UPDATE_STATUS_API = self.UPDATE_STATUS_API
        options = {
            'linkage': self.option('method'),
            'dis_matrix': self.option('distance_matrix')
        }
        task.set_options(options)
        task.on('end', self.set_db)
        task.run()
        self.output_dir = task.output_dir
        super(HclusterWorkflow, self).run()

    def set_db(self):
        """
        保存结果树结果到mongo数据库中
        """
        api_newick = self.api.newicktree
        newick_fath = self.output_dir + "/hcluster.tre"
        if not os.path.isfile(newick_fath):
            raise Exception("找不到报告文件:{}".format(newick_fath))
        api_newick.add_tree_file(newick_fath, self.option('newick_id'))
        self.end()
