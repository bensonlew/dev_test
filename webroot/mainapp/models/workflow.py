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
        where_dict = dict(workflow_id=wid)
        t = self.db.transaction()
        try:
            self.db.update(self.table, where=web.db.sqlwhere(where_dict), is_error=1, error=insert_data["reason"],
                           is_end=1,end_time=datetime.datetime.now())
            self.db.insert("tstop", workflow_id=wid, **insert_data)
        except Exception:
            t.rollback()
            return False
        else:
            t.commit()
            return True

    def get_running(self, client):
        where_dict = dict(client=client, has_run=1, is_end=0, is_error=0)
        return self.db.select(self.table, where=web.db.sqlwhere(where_dict))

    def get_queue(self, client):
        where_dict = dict(client=client, has_run=0)
        return self.db.select(self.table, where=web.db.sqlwhere(where_dict))
