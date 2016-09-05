# -*- coding: utf-8 -*-
# __author__ = 'shenghe'

"""距离矩阵层级聚类"""

import datetime
from biocluster.workflow import Workflow
from bson import ObjectId
import os


class HclusterWorkflow(Workflow):
    """
    报告中调用距离矩阵计算样本层级聚类数使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(HclusterWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "distance_matrix", "type": "infile", "format": "meta.beta_diversity.distance_matrix"},
            {"name": "method", "type": "string", "default": 'average'},
            {"name": "update_info", "type": "string"},
            {"name": "distance_id", "type": "string"},
            {"name": "newick_id", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())

    def run(self):
        task = self.add_tool("meta.beta_diversity.hcluster")
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
        collection = api_newick.db["sg_beta_specimen_distance"]
        result = collection.find_one({"_id": ObjectId(self.option('distance_id'))})
        task_id = result['task_id']
        newick_fath = self.output_dir + "/hcluster.tre"
        if not os.path.isfile(newick_fath):
            raise Exception("找不到报告文件:{}".format(newick_fath))
        params = {
            'specimen_distance_id': self.option('distance_id'),
            'hcluster_method': self.option('method'),
            'submit_location': self.option('submit_location'),
            'task_type': self.option('task_type')
            }
        return_id = api_newick.add_tree_file(newick_fath, major=True, table_id=self.option('distance_id'),
                                             task_id=task_id, table_type='dist', tree_type='cluster',
                                             name='hcluset_{}_{}'.format(self.option('method'), datetime.datetime.now().strftime("%Y%m%d_%H%M%S")), params=params)
        self.add_return_mongo_id('sg_newick_tree', return_id)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "层次聚类结果目录"],
            ["./hcluster.tre", "tre", "层次聚类树"]
            ])
        super(HclusterWorkflow, self).end()
