# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
# from bson.son import SON
from types import StringTypes


class Hcluster(Base):
    def __init__(self, bind_object):
        super(Hcluster, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_sample_newicktree(self, file_path, newicktree_id, json_data={}, desc=None):
        with open(file_path, 'r') as f:
            oneline = f.readline()
            if not re.match(r'^\(.*\)\;$', oneline):
                raise Exception('文件%s格式不正确，请选择正确的newick tree树文件' % file_path)
            update_data = {
                'value': oneline
            }
            collection = self.db["sg_newick_tree"]
            if isinstance(newicktree_id, StringTypes):
                newicktree_id = ObjectId(newicktree_id)
            elif isinstance(newicktree_id, ObjectId):
                pass
            else:
                raise Exception("输入newicktree_id参数必须为字符串或者ObjectId类型!")
            try:
                collection.update_one({'_id': newicktree_id}, update_data)
            except Exception, e:
                self.bind_object.logger.error("导入newicktree:%s信息出错:%s" % (file_path, e))
            else:
                self.bind_object.logger.info("导入newicktree:%s信息成功!" % file_path)
