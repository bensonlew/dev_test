# -*- coding: utf-8 -*-
# __author__ = 'yuguo'
from biocluster.api.database.base import Base, report_check
# import re
from bson.objectid import ObjectId
import datetime
import json
# from biocluster.config import Config
from mainapp.libs.param_pack import group_detail_sort
# from bson.son import SON
# from types import StringTypes


class Newicktree(Base):
    def __init__(self, bind_object):
        super(Newicktree, self).__init__(bind_object)
        self._project_type = 'meta'
        # self._db_name = Config().MONGODB

    @report_check
    def add_tree_file(self, file_path, major=False, level=None, task_id=None, table_id=None, table_type=None,
                      tree_type=None, tree_id=None, name=None, params=None, spname_spid=None, update_dist_id=None):
        if major:
            if table_type not in ('otu', 'dist', 'other'):
                raise Exception("table_type参数必须为'otu', 'dist', 'other'")
            if tree_type not in ('phylo', 'cluster'):
                raise Exception("tree_type参数必须为'phylo', 'cluster'")
            if table_type == "otu":
                if level not in range(1, 10):
                    raise Exception("table_type为'otu'时，level参数%s为不在允许范围内!" % level)
            if task_id is None:
                task_id = self.bind_object.sheet.id
            if not isinstance(table_id, ObjectId) and table_id is not None:
                table_id = ObjectId(table_id)
            if table_type == 'otu':
                if params:
                    params['otu_id'] = str(table_id)  # 不管是矩阵聚类还是序列进化树，的table_id都是 otu_id
                if spname_spid:
                    group_detail = {'All': [str(i) for i in spname_spid.values()]}
                    params['group_detail'] = group_detail_sort(group_detail)
            insert_data = {
                "project_sn": self.bind_object.sheet.project_sn,
                "task_id": task_id,
                "table_id": table_id,
                "table_type": table_type,
                "level_id": level,
                "name": "Tree_Origin",
                "tree_type": tree_type,
                "status": "end",
                "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            try:
                collection = self.db["sg_newick_tree"]
                tree_id = collection.insert_one(insert_data).inserted_id
            except Exception, e:
                self.bind_object.logger.error("导入newick%s信息出错:%s" % (file_path, e))
            else:
                self.bind_object.logger.info("导入newick%s信息成功!" % file_path)
        else:
            if tree_id is None:
                raise Exception("major为False时需提供tree_id!")
            if not isinstance(tree_id, ObjectId):
                tree_id = ObjectId(tree_id)
        # update value
        self.bind_object.logger.info('update_dist_id: {}'.format(update_dist_id))
        if update_dist_id:
            self.update_dist(update_dist_id, tree_id)
        with open(file_path, 'r') as f:
            line = f.readline()
            try:
                collection = self.db["sg_newick_tree"]
                collection.update_one({"_id": tree_id}, {"$set": {"value": line}})
            except Exception, e:
                self.bind_object.logger.error("导入newick tree%s信息出错:%s" % (file_path, e))
            else:
                self.bind_object.logger.info("导入newick tree%s信息成功!" % file_path)
        return tree_id

    def update_dist(self, distance_id, tree_id):
        """
        从newick树更新距离矩阵结果的主表的newick_tree_id
        """
        self.db['sg_beta_specimen_distance'].update_one({'_id': ObjectId(distance_id)},
                                                        {'$set': {'newick_tree_id': ObjectId(tree_id)}})
