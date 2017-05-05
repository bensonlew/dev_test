# -*- coding: utf-8 -*-
# __author__ = 'shijin'

from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
import os
from bson import SON
import datetime

class FastqExtract(Base):
    def __init__(self, bind_object):
        super(FastqExtract, self).__init__(bind_object)
        self.output_dir = self.bind_object.output_dir
        self.work_dir = self.bind_object.work_dir
        if Config().MONGODB == 'sanger':
            self._db_name = 'toolapps'
        else:
            self._db_name = 'ttoolapps'
        self.check()

    def check(self):
        pass

    @report_check
    def run(self):
        """
        运行函数
        """
        self.export2database()

    def export2database(self):
        results_list = []
        with open(self.work_dir + "/FastqExtract/info.txt") as r:
            main_id = self.db['fastq_extract'].insert_one(SON(
                project_sn=self.bind_object.sheet.project_sn,
                task_id=self.bind_object.id,
                name='fastq_extract',
                desc='样本拆分表',
                status='succeed',
                created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )).inserted_id
            for line in r:
                result = {}
                tmp = line.strip().split("\t")
                result["main_id"] = main_id
                result["name"] = os.path.basename(tmp[0])
                result["sample"] = tmp[1]
                result["seq_num"] = tmp[3]
                result["base_num"] = tmp[4]
                result["average"] = tmp[5]
                result["min"] = tmp[6]
                result["max"] = tmp[7]
                results_list.append(result)
        self.db['fastq_extract_detail'].insert_many(results_list)
        self.bind_object.logger.info("导表成功")
