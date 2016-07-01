# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.api.database.base import Base, report_check
from bson.objectid import ObjectId
from types import StringTypes


class HeatCluster(Base):
    def __init__(self, bind_object):
        super(HeatCluster, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def update_newick(self, path, newick_id):
        if not isinstance(newick_id, ObjectId):
            if isinstance(newick_id, StringTypes):
                newick_id = ObjectId(newick_id)
            else:
                raise Exception("newick_id必须为ObjectId对象或其对应的字符串!")
        with open(path, 'rb') as r:
            line = r.readline().strip('\r\n')
            samples = self.bind_object.hcluster.option("newicktree").prop['sample']
        insert_data = {
            "value": line,
            "samples": samples
        }
        collection = self.db['sg_newick_tree']
        try:
            collection.find_one_and_update({"_id": newick_id}, {"$set": insert_data})
        except Exception as e:
            self.bind_object.logger.error("导入newick tree%s信息出错:%s" % (path, e))
        else:
            self.bind_object.logger.info("导入newick tree%s信息成功!" % path)
