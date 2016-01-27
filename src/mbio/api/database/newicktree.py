# -*- coding: utf-8 -*-
# __author__ = 'yuguo'
from biocluster.api.database.base import Base, report_check
# import re
# from bson.objectid import ObjectId
import datetime
# from bson.son import SON
# from types import StringTypes


class Newicktree(Base):
    def __init__(self, bind_object):
        super(Newicktree, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_tree_file(self, file_path, level, task_id=None, table_id=None, table_type=None, tree_type=None, name=None, params=None):
        if table_type == "otu":
            if level not in range(1, 10):
                raise Exception("level参数%s为不在允许范围内!" % level)
        if task_id is None:
            task_id = self.bind_object.sheet.id
        with open(file_path, 'r') as f:
            line = f.readline()
            insert_data = {
                "project_sn": self.bind_object.sheet.project_sn,
                "task_id": task_id,
                "table_id": table_id,
                "table_type": table_type,
                "level": level,
                "name": name if name else "tree_origin",
                "tree_type": tree_type,
                "value": line,
                "status": "end",
                # "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            try:
                collection = self.db["sg_newick_tree"]
                collection.insert_one(insert_data)
            except Exception, e:
                self.bind_object.logger.error("导入newick%s信息出错:%s" % (file_path, e))
            else:
                self.bind_object.logger.info("导入newick%s信息成功!" % file_path)
