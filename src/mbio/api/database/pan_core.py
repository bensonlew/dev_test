# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes


class PanCore(Base):
    def __init__(self, bind_object):
        super(PanCore, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_pan_core_detail(self, file_path, pan_core_id, category_name):
        if not isinstance(pan_core_id, ObjectId):
            if isinstance(pan_core_id, StringTypes):
                pan_core_id = ObjectId(pan_core_id)
            else:
                raise Exception("pan_core_id必须为ObjectId对象或其对应的字符串!")
        with open(file_path, 'rb') as r:
            header = r.next().rstrip("\n")
            header.pop(0)
            pan_core_detail = list()
            for line in r:
                line = line.rstrip('\n')
                line = re.split('\t', line)
                value = list()
                length = len(line)
                for i in range(1, length):
                    my_dic = dict()
                    my_dic["title"] = header[i]
                    my_dic["value"] = line[i]
                    value.append(my_dic)
                insert_data = {
                    "pan_core_id": pan_core_id,
                    "category_name": line[0],
                    "value": value
                }
                pan_core_detail.append(insert_data)
            try:
                collection = self.db['sg_otu_pan_core_detail']
                collection.insert_many(pan_core_detail)
            except Exception as e:
                self.bind_object.logger.error("导入pan_core_detail表格{}信息出错:{}".format(file_path, e))
            else:
                self.bind_object.logger.info("导入pan_core_detail表格{}成功".format(file_path))
