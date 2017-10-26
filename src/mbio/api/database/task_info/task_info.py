# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import datetime
from biocluster.api.database.base import Base, report_check
# from biocluster.config import Config
from bson import SON


class TaskInfo(Base):
    def __init__(self, bind_object):
        super(TaskInfo, self).__init__(bind_object)
        self._project_type = 'meta'
        # self._db_name = Config().MONGODB

    @report_check
    def add_task_info(self, db_name=None):
        if db_name:
            self._db_name = db_name
        json_data = [
            ('task_id', self.bind_object.sheet.id),
            ('member_id', self.bind_object.sheet.member_id),
            ('project_sn', self.bind_object.sheet.project_sn),
            ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ]
        self.db['sg_task'].insert_one(SON(json_data))
        self.bind_object.logger.info('任务信息导入sg_task成功。')
