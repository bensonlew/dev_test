# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'

from biocluster.api.database.base import Base, report_check
import re
import datetime
from bson import ObjectId, SON
from bson.objectid import ObjectId
from types import StringTypes
from biocluster.config import Config


class MetaSourcetracker(Base):
    """
    微生物来源组成比例分析
    """
    def __init__(self, bind_object):
        super(MetaSourcetracker, self).__init__(bind_object) #
        self._db_name = Config().MONGODB
        self.task_id = ""

    @report_check
    def add_sg_sourcetracker_detail(self, id=None, file_path=None, name=None):
        insert_data = list()
        with open(file_path, 'rb') as r:
            head = r.next().strip('\r\n')  # windows换行符
            head = re.split('\t', head)
            new_head = head[1:]
            for line in r:
                line = line.rstrip("\r\n")
                line = re.split('\t', line)
                group_num = line[1:]
                # classify_list = re.split(r"\s*;\s*", line[0])
                detail = dict()
                detail['sourcetracker_id'] = ObjectId(id)
                detail['file_name'] = name
                detail['sample_name'] = line[0]
                # for cf in classify_list:
                #     if cf != "":
                #         detail[cf[0:3].lower()] = cf
                for i in range(0, len(group_num)):
                    detail[new_head[i]] = group_num[i]
                insert_data.append(detail)
        try:
            collection = self.db['sg_sourcetracker_detail']
            collection.insert_many(insert_data)
        except Exception as e:
            self.bind_object.logger.error("导入sg_sourcetracker_detail表格信息出错:{}".format(e))
        else:
            self.bind_object.logger.info("导入sg_sourcetracker_detail表格成功")