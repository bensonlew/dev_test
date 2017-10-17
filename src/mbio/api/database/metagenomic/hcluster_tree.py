# -*- coding: utf-8 -*-
# __author__ = 'zouxuan'
# last_modify:20170926
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
import json
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId



class HclusterTree(Base):
    def __init__(self, bind_object):
        super(HclusterTree, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_hcluster_tree(self, file_path, main=False, tree_id=None, task_id=None, specimen_group=None, geneset_id=None,
                          anno_id=None,
                          level_id=None,
                          params=None, update_dist_id=None):
        if main:
            if task_id is None:
                task_id = self.bind_object.sheet.id
            project_sn = self.bind_object.sheet.project_sn
            created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            insert_data = {
                'project_sn': project_sn,
                'task_id': task_id,
                'desc': '',
                'created_ts': created_ts,
                'name': 'null',
                'params': json.dumps(params, sort_keys=True, separators=(',', ':')),
                'status': 'end',
                'geneset_id': geneset_id,
                'anno_id': anno_id,
                'specimen_group': specimen_group,
                'level_id': level_id
            }
            collection = self.db['hcluster_tree']
            # 将主表名称写在这里
            hcluster_tree_id = collection.insert_one(insert_data).inserted_id
        else:
            if tree_id is None:
                raise Exception("main为False时需提供tree_id!")
            if not isinstance(tree_id, ObjectId):
                tree_id = ObjectId(tree_id)
        if update_dist_id:
            self.update_dist(update_dist_id, tree_id)
        with open(file_path, 'r') as f:
            line = f.readline()
            try:
                collection = self.db["hcluster_tree"]
                collection.update_one({"_id": tree_id}, {"$set": {"specimen_tree": line}})
            except Exception, e:
                self.bind_object.logger.error("导入hcluster tree%s信息出错:%s" % (file_path, e))
            else:
                self.bind_object.logger.info("导入hcluster tree%s信息成功!" % file_path)
        return tree_id

    @report_check
    def update_dist(self, distance_id, tree_id):
        """
        从newick树更新距离矩阵结果的主表的newick_tree_id
        """
        self.db['specimen_distance'].update_one({'_id': ObjectId(distance_id)},
                                                {'$set': {'hcluster_tree_id': ObjectId(tree_id)}})
