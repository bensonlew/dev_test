# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from ..core.function import hostname
from .mysql import Mysql
import json
from ..core.function import CJsonEncoder
import traceback
import sys
import time


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

    def __del__(self):
        self._db.close()

    def close(self):
        self._db.close()

    def save(self, pid=0):
        """
        添加workflow记录到表格中
        """
        try:
            is_instant = 1 if self.sheet.instant else 0
            sql = "INSERT INTO workflow (client, workflow_id, json, server, pid, instant, path, type, batch_id) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

            data = (self.sheet.client, self.workflow_id, json.dumps(self.sheet.data, cls=CJsonEncoder),
                    hostname, pid, str(is_instant), self.sheet.name, self.sheet.type, self.sheet.batch_id)

            count = self._db.insert_one(sql, data)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            print e
            sys.stdout.flush()
        else:
            return count

    def update_pid(self, pid):
        sql = "update workflow set has_run=1, run_time=CURRENT_TIMESTAMP(), pid=%s " \
              "where workflow_id = %s"
        # print sql
        return self._db.update(sql, (pid, self.workflow_id))

    def find(self):
        sql = "select json from workflow where workflow_id = %s"
        # print sql
        return self._db.get_one(sql, (self.workflow_id, ))

    def update(self):
        sql = "update workflow set last_update =CURRENT_TIMESTAMP() " \
              "where workflow_id = %s"
        # print sql
        return self._db.update(sql, (self.workflow_id, ))

    def error(self, error_msg):
        sql = "update workflow set is_end=1,is_error=1, error=%s, end_time=CURRENT_TIMESTAMP() " \
              "where workflow_id = %s"
        data = (error_msg, self.workflow_id)
        # print sql
        return self._db.update(sql, data)

    def end(self):
        sql = "update workflow set is_end=1, end_time=CURRENT_TIMESTAMP() " \
            "where workflow_id = %s"
        # print sql
        return self._db.update(sql, (self.workflow_id, ))

    def stop(self):
        sql = "update tostop set done=1, stoptime=CURRENT_TIMESTAMP() " \
              "where done=0 and workflow_id = %s"
        # print sql
        return self._db.update(sql, (self.workflow_id, ))

    def pause(self):
        try:
            self._db.cursor.execute("SET AUTOCOMMIT = 0")
            sql1 = "update pause set has_pause=1, pause_time=CURRENT_TIMESTAMP() " \
                   "where workflow_id = %s and has_pause=0"
            self._db.query(sql1, (self.workflow_id,))
            sql2 = "update workflow set paused=1 where workflow_id = %s"
            self._db.query(sql2, (self.workflow_id,))
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            print e
            sys.stdout.flush()
            self._db.end(option="rollback")
        else:
            self._db.end()
        self._db.cursor.execute("SET AUTOCOMMIT = 1")

    def exit_pause(self):
        try:
            self._db.cursor.execute("SET AUTOCOMMIT = 0")
            sql1 = "update pause set has_continue=1,continue_time=CURRENT_TIMESTAMP() " \
                   "where workflow_id = %s and has_pause=1 and exit_pause=1 and has_continue=0"
            self._db.query(sql1, (self.workflow_id, ))
            sql2 = "update workflow set paused=0 where workflow_id = %s"
            self._db.query(sql2, (self.workflow_id, ))
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            print e
            sys.stdout.flush()
            self._db.end(option="rollback")
        else:
            self._db.end()
        self._db.cursor.execute("SET AUTOCOMMIT = 1")

    def pause_timeout(self):
        try:
            self._db.cursor.execute("SET AUTOCOMMIT = 0")
            sql1 = "update pause set timeout=1,timeout_time=CURRENT_TIMESTAMP() " \
                   "where workflow_id = %s and has_pause=1 and exit_pause=0 and timeout=0"
            self._db.query(sql1, (self.workflow_id,))
            sql2 = "update workflow set paused=0 where workflow_id = %s"
            self._db.query(sql2, (self.workflow_id,))
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            print e
            sys.stdout.flush()
            self._db.end(option="rollback")
        else:
            self._db.end()
        self._db.cursor.execute("SET AUTOCOMMIT = 1")


class CheckModel(object):
    def __init__(self):
        self._db = Mysql()

    def __del__(self):
        self._db.close()

    def close(self):
        self._db.close()

    def find_stop(self):
        sql = "select workflow_id from tostop where done=0 and time > DATE_SUB(now(),INTERVAL 1 hour)"
        return self._db.get_all(sql)

    def find_pause(self):
        sql = "select workflow_id from pause where has_pause=0 and add_time > DATE_SUB(now(),INTERVAL 1 hour)"
        # print sql
        return self._db.get_all(sql)

    def find_exit_pause(self):
        sql = "select workflow_id from pause where has_pause=1 and exit_pause=1 and has_continue=0 and timeout=0 and " \
              "exit_pause_time > DATE_SUB(now(),INTERVAL 1 hour)"
        # print sql
        return self._db.get_all(sql)


class ApiLogModel(object):
    def __init__(self, log_object):
        self._db = Mysql()
        self.log_object = log_object

    def __del__(self):
        self._db.close()

    def close(self):
        self._db.close()

    def save(self):
        is_success = 1 if self.log_object.web_api_success else 0
        has_update_status = 1 if self.log_object.has_update_status else 0
        update_status_success = 1 if self.log_object.update_status_success else 0
        has_update_webapi = 1 if self.log_object.has_update_webapi else 0

        sql = "INSERT INTO apilog (task_id, api, data, update_status, update_status_success, webapi, " \
              "success, server, response, response_code) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        data = (self.log_object.task_id, self.log_object.api, json.dumps(self.log_object.data, cls=CJsonEncoder),
                has_update_status, update_status_success, has_update_webapi, is_success, hostname,
                self.log_object.response, self.log_object.response_code)
        return self._db.insert_one(sql, data)


class ClientKeyModel(object):
    def __init__(self):
        self._db = Mysql()

    def __del__(self):
        self._db.close()

    def close(self):
        self._db.close()

    def find_key(self, client):
        sql = "select key from clientkey where client = %s"
        data = self._db.get_one(sql, (client, ))
        if data:
            return data["key"]
        else:
            return None


class ReportModel(object):

    def __init__(self, wid):
        self._db = Mysql()
        self.workflow_id = wid
        # self.auto_id = self.get_workflow_id()

    # def get_workflow_id(self):
    #     sql = "select id from workflow where workflow_id = %s"
    #     result = self._db.get_one(sql, (self.workflow_id,))
    #     if result and "id" in result.keys():
    #         return result["id"]
    #     else:
    #         return 0
    def __del__(self):
        self._db.close()

    def close(self):
        self._db.close()

    def save_modules(self, data):
        try:
            self._db.cursor.execute("SET AUTOCOMMIT = 0")
            for d in data:
                sql = "insert into module (parent_run_id, run_id, path, work_dir, start_time, end_time, tool_num) " \
                      "values (%s, %s, %s, %s, %s, %s, %s)"
                data = (self.workflow_id, d["run_id"], d["path"], d["work_dir"],
                        time.strftime("%Y-%m-%d %X", time.localtime(d["start_time"])),
                        time.strftime("%Y-%m-%d %X", time.localtime(d["end_time"])),
                        d["tool_num"])
                self._db.cursor.execute(sql, data)
                # self._db.cursor.execute("SELECT @@IDENTITY AS id")
                # result = self._db.cursor.fetchall()
                # wid = result[0]['id']
                if len(d["tools"]) > 0:
                    self.save_tools(d["run_id"], d["tools"])
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            print e
            sys.stdout.flush()
            self._db.end(option="rollback")
        else:
            self._db.end()
        self._db.cursor.execute("SET AUTOCOMMIT = 1")

    def save_tools(self, parent_run_id, data, commit=False, under_workflow=0):
        try:
            if not commit:
                self._db.cursor.execute("SET AUTOCOMMIT = 0")
            for d in data:
                sql = "insert into tool (parent_run_id, run_id, path, work_dir, run_host, job_type, job_id, " \
                      "request_cpu, request_memory, start_time, wait_spend_time, queue_spend_time,  run_spend_time, " \
                      "end_time, run_times, success, info, under_workflow) values " \
                      "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                data = (parent_run_id, d["run_id"], d["path"], d["work_dir"], d["run_host"], d["job_type"], d["job_id"],
                        d["request_cpu"], d["request_memory"],
                        time.strftime("%Y-%m-%d %X", time.localtime(d["start_time"])),
                        d["wait_spend_time"], d["queue_spend_time"], d["run_spend_time"],
                        time.strftime("%Y-%m-%d %X", time.localtime(d["end_time"])),
                        d["run_times"], d["success"], d["info"], under_workflow)
                self._db.cursor.execute(sql, data)
                # self._db.cursor.execute("SELECT @@IDENTITY AS id")
                # result = self._db.cursor.fetchall()
                # tid = result[0]['id']
                if len(d["commands"]) > 0:
                    self.save_commands(d["run_id"], d["commands"])
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            print e
            sys.stdout.flush()
            if commit:
                self._db.end(option="rollback")
        else:
            if commit:
                self._db.end()
        self._db.cursor.execute("SET AUTOCOMMIT = 1")

    def save_commands(self, parent_run_id, data):
        for d in data:
            sql = "insert into command (parent_run_id, name, cmd, start_time, end_time, run_times, main_pid, " \
                  "sub_process_num, max_cpu_use, max_rss, average_cpu_use, average_rss, return_code, max_vms, " \
                  "average_vms) values " \
                  "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data = (parent_run_id, d["name"], d["cmd"], time.strftime("%Y-%m-%d %X", time.localtime(d["start_time"])),
                    time.strftime("%Y-%m-%d %X", time.localtime(d["end_time"])),
                    d["run_times"], d["main_pid"], d["sub_process_num"],  d["max_cpu_use"],
                    d["max_rss"], d["average_cpu_use"], d["average_rss"], d["return_code"], d["max_vms"],
                    d["average_vms"])
            self._db.cursor.execute(sql, data)
