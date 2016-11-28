# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from mainapp.config.db import DB
import web
import datetime


class Workflow(object):
    def __init__(self):
        self.table = "workflow"
        self.db = DB

    def get_by_workflow_id(self, wid):
        where_dict = dict(workflow_id=wid)
        return self.db.select(self.table, where=web.db.sqlwhere(where_dict))

    def add_record(self, data):
        return self.db.insert(self.table, **data)

    def last_update_seconds(self, wid):
        data = self.db.query("SELECT TIMESTAMPDIFF(SECOND,last_update,CURRENT_TIMESTAMP) as diff"
                             " FROM workflow where workflow_id=$id", vars={'id': wid})
        return data[0].diff

    def set_stop(self, wid, insert_data):
        try:
            results = self.db.query("SELECT * FROM tostop WHERE workflow_id=$id", vars={'id': wid})
            if len(results) > 0:
                insert_data["time"] = datetime.datetime.now()
                insert_data["done"] = 0
                self.db.update("tostop", vars={'id': wid}, where="workflow_id = $id", **insert_data)
            else:
                self.db.insert("tostop", workflow_id=wid, **insert_data)
        except:
            return False
        else:
            return True

    def get_running(self, client):
        where_dict = dict(client=client, has_run=1, is_end=0, is_error=0)
        return self.db.select(self.table, where=web.db.sqlwhere(where_dict))

    def get_queue(self, client):
        where_dict = dict(client=client, has_run=0)
        return self.db.select(self.table, where=web.db.sqlwhere(where_dict))

    def set_pause(self, wid, insert_data):
        try:
            results = self.db.query("SELECT * FROM pause WHERE workflow_id=$id", vars={'id': wid})
            if len(results) > 0:
                insert_data["add_time"] = datetime.datetime.now()
                insert_data["has_pause"] = 0
                insert_data["exit_pause"] = 0
                insert_data["has_continue"] = 0
                insert_data["timeout"] = 0
                self.db.update("pause", vars={'id': wid}, where="workflow_id = $id", **insert_data)
            else:
                self.db.insert("pause", workflow_id=wid, **insert_data)
        except:
            return False
        else:
            return True

    def exit_pause(self, wid):
        try:
            results = self.db.query("SELECT * FROM pause WHERE workflow_id=$id and has_pause=1", vars={'id': wid})
            if len(results) > 0:
                update_data = {
                    "exit_pause": 1,
                    "exit_pause_time": datetime.datetime.now()
                }
                self.db.update("pause", vars={'id': wid}, where="workflow_id = $id", **update_data)
        except:
            return False
        else:
            return True
