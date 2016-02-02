# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes


class Venn(Base):
    def __init__(self, bind_object):
        super(Venn, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_venn_detail(self, file_path, venn_id):
        if not isinstance(pan_core_id, ObjectId):
            if isinstance(pan_core_id, StringTypes):
                venn_id = ObjectId(venn_id)
            else:
                raise Exception("venn_id必须为ObjectId对象或其对应的字符串!")
        with open(file_path, 'rb') as r:
            for line in r:
                line = line.rsplit('\n')
                line = re.split("\t", line)

