# -*- coding: utf-8 -*-
# __author__ = 'shijin'

from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config


class GenomeInfo(Base):
    def __init__(self, bind_object=None):
        super(GenomeInfo, self).__init__(bind_object)
        self._db_name = Config().MONGODB + "_ref_rna"

    @report_check
    def add_genome_info(self, ):
        main_insert_data = {
            'project_sn': self.bind_object.sheet.project_sn,
            'task_id': self.bind_object.sheet.id,
            'name': "",
            'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        main_collection = self.db['sg_genome_info']
        collections = self.db['sg_genome_info_detail']
        data_list = []
        main_collection.insert_one(main_insert_data)

