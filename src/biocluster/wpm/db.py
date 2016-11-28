# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from ..core.function import hostname
from .mysql import Mysql


class WorkflowModel(object):
    """
    操作数据库workflow表
    """
    def __init__(self, process):
        """

        :param process: WorkflowWorker 对象
        """
        self._db = Mysql()
        self.process = process
        self.workflow_id = process.wsheet.id
        self.sheet = process.wsheet

    def save(self):
        """
        添加workflow记录到表格中
        """
        sql = "INSERT INTO workflow (client, workflow_id, json, server, pid) VALUES " \
              "('" + self.sheet.client + "', '" \
              + self.workflow_id + "', '" + self.sheet.data + "','" + hostname + "', '" + self.process.pid + "')"
        self._db.query(sql)

    def find(self):
        sql = "select json from workflow where workflow_id = '" + self.process.wsheet.id + "'"
        data = self._db.select(sql)
        if data:
            return data
        else:
            return False

    def update(self):
        sql = "update workflow set last_update =CURRENT_TIMESTAMP() " \
              "where workflow_id = '" + self.process.wsheet.id + "'"
        return self._db.query(sql)

    def error(self, error_msg):
        sql = "update workflow set end=1,is_error=1, error='" + error_msg + "', end_time=CURRENT_TIMESTAMP() " \
              "where workflow_id = '" + self.process.wsheet.id + "'"
        return self._db.query(sql)

    def end(self):
        sql = "update workflow set end=1, end_time=CURRENT_TIMESTAMP() " \
            "where workflow_id = '" + self.process.wsheet.id + "'"
        return self._db.query(sql)

    def pause(self):
        sql = "update pause set has_pause=1, pause_time=CURRENT_TIMESTAMP()" \
              "where workflow_id = '" + self.process.wsheet.id + "' and has_pause=0;"
        sql += "update workflow set paused=1 where workflow_id = '" + self.process.wsheet.id + "'"
        return self._db.query(sql)

    def exit_pause(self):
        sql = "update pause set has_continue=1,continue_time=CURRENT_TIMESTAMP() " \
              "where workflow_id = '" + self.process.wsheet.id + "' and has_pause=1 " \
                                                                 "and exit_pause=1 and has_continue=0;"
        sql += "update workflow set paused=0 where workflow_id = '" + self.process.wsheet.id + "'"
        return self._db.query(sql)

    def pause_timeout(self):
        sql = "update pause set timeout=1,timeout_time=CURRENT_TIMESTAMP() " \
              "where workflow_id = '" + self.process.wsheet.id + "' and has_pause=1 " \
                                                                 "and exit_pause=0 and timeout=0;"

        sql += "update workflow set paused=0 where workflow_id = '" + self.process.wsheet.id + "'"
        return self._db.query(sql)
