# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
# last_modify:20161205

import os
import datetime
from bson.son import SON
from bson.objectid import ObjectId
import types
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
import json


class RefRnaGeneset(Base):
    def __init__(self, bind_object):
        super(RefRnaGeneset, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_ref_rna'

    @report_check
    def add_geneset_cog_detail(self, geneset_cog_table, geneset_cog_id=None):
        data_list = []
        with open(geneset_cog_table, 'r') as f:
            first_line = f.readline().strip().split("\t")
            print first_line
            geneset_name = []
            for gn in first_line[2:]:
                if not gn[:-4] in geneset_name:
                    geneset_name.append(gn[:-4])
            print geneset_name
            for line in f:
                line = line.strip().split("\t")
                data = {
                    'geneset_cog_id': geneset_cog_id,
                    'type': line[0],
                    'function_categories': line[1]
                }
                for n, gn in enumerate(geneset_name):
                    data["geneset_name"] = gn
                    data["cog"] = int(line[n+2])
                    data["nog"] = int(line[n+2])
                    data["kog"] = int(line[n+2])
                print data
                # data = SON(data)
                data_list.append(data)
            try:
                collection = self.db['sg_geneset_cog_class_detail']
                collection.insert_many(data_list)
            except Exception, e:
                self.bind_object.logger.error("导入cog表格：%s出错:%s" % (geneset_cog_table, e))
            else:
                self.bind_object.logger.error("导入cog表格：%s成功!" % (geneset_cog_table))
