# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from ..core.function import hostname
from .mysql import Mysql
import MySQLdb
import json
from ..core.function import CJsonEncoder


class WorkflowModel(object):
    """
    操作数据库workflow表
    """
    def __init__(self, wsheet):
        """

        :param wsheet: sheet对象
        """
        self._db = Mysql()
        self.workflow_id = wsheet.id
        self.sheet = wsheet

    def save(self, pid=0):
        """
        添加workflow记录到表格中
        """
        is_instant = 1 if self.sheet.instant else 0
        sql = "INSERT INTO workflow (client, workflow_id, json, server, pid, instant) VALUES " \
              "('%s', '%s', '%s', '%s', %s, %s)" % \
              (self.sheet.client, self.workflow_id,
               MySQLdb.escape_string(json.dumps(self.sheet.data, cls=CJsonEncoder)),
               hostname, pid, str(is_instant))
        # print sql
        self._db.query(sql)

    def update_pid(self, pid):
        sql = "update workflow set has_run=1, run_time=CURRENT_TIMESTAMP(), pid=%s " \
              "where workflow_id = '%s'" % (pid, self.workflow_id)
        # print sql
        return self._db.query(sql)

    def find(self):
        sql = "select json from workflow where workflow_id = '%s'" % self.workflow_id
        # print sql
        data = self._db.select(sql)
        if data:
            return data
        else:
            return False

    def update(self):
        sql = "update workflow set last_update =CURRENT_TIMESTAMP() " \
              "where workflow_id = '%s'" % self.workflow_id
        # print sql
        return self._db.query(sql)

    def error(self, error_msg):
        sql = "update workflow set is_end=1,is_error=1, error='%s', end_time=CURRENT_TIMESTAMP() " \
              "where workflow_id = '%s'" % (MySQLdb.escape_string(error_msg), self.workflow_id)
        # print sql
        return self._db.query(sql)

    def end(self):
        sql = "update workflow set is_end=1, end_time=CURRENT_TIMESTAMP() " \
            "where workflow_id = '%s'" % self.workflow_id
        # print sql
        return self._db.query(sql)

    def stop(self):
        sql = "update tostop set done=1, stoptime=CURRENT_TIMESTAMP() " \
              "where done=0 and workflow_id = '%s'" % self.workflow_id
        # print sql
        return self._db.query(sql)

    def pause(self):
        sql1 = "update pause set has_pause=1, pause_time=CURRENT_TIMESTAMP() " \
              "where workflow_id = '%s' and has_pause=0" % self.workflow_id
        sql2 = "update workflow set paused=1 where workflow_id = '%s'" % self.workflow_id
        # print sql
        return self._db.transaction(sql1, sql2)

    def exit_pause(self):
        sql1 = "update pause set has_continue=1,continue_time=CURRENT_TIMESTAMP() " \
              "where workflow_id = '%s' and has_pause=1 and exit_pause=1 and has_continue=0" % self.workflow_id
        sql2 = "update workflow set paused=0 where workflow_id = '%s'" % self.workflow_id
        # print sql
        return self._db.transaction(sql1, sql2)

    def pause_timeout(self):
        sql1 = "update pause set timeout=1,timeout_time=CURRENT_TIMESTAMP() " \
              "where workflow_id = '%s' and has_pause=1 and exit_pause=0 and timeout=0" % self.workflow_id
        sql2 = "update workflow set paused=0 where workflow_id = '%s'" % self.workflow_id
        # print sql
        return self._db.transaction(sql1, sql2)


class CheckModel(object):
    def __init__(self):
        self._db = Mysql()

    def find_stop(self):
        sql = "select workflow_id from tostop where done=0"
        # print sql
        return self._db.select(sql)

    def find_pause(self):
        sql = "select workflow_id from pause where has_pause=0"
        # print sql
        return self._db.select(sql)

    def find_exit_pause(self):
        sql = "select workflow_id from pause where has_pause=1 and exit_pause=1 and has_continue=0 and timeout=0"
        # print sql
        return self._db.select(sql)


class ApiLogModel(object):
    def __init__(self, log_object):
        self._db = Mysql()
        self.log_object = log_object

    def save(self):
        is_success = 0 if self.log_object.failed else 1
        sql = "INSERT INTO apilog (task_id, api, data, success, server, response, response_code) VALUES " \
              "('%s', '%s', '%s', %s, '%s', '%s', %s)"  \
              % (self.log_object.task_id, self.log_object.api,
                 MySQLdb.escape_string(json.dumps(self.log_object.data, cls=CJsonEncoder)), is_success, hostname,
                 MySQLdb.escape_string(self.log_object.response), self.log_object.response_code)
        # print sql
        self._db.query(sql)
