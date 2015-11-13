# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from mainapp.config.db import DB
import web


class Workflow(object):
    def __init__(self):
        self.table = "workflow"
        self.db = DB

    def get_by_workflow_id(self, wid):
        where_dict = dict(workflow_id=wid)
        return self.db.select(self.table, where=web.db.sqlwhere(where_dict))

    def add_record(self, data):
        return self.db.insert(self.table, **data)

