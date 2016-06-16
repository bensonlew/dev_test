# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from mainapp.config.db import DB
import web


class InstantTask(object):
    def __init__(self):
        self.table = "instantTask"
        self.db = DB

    def GetByTaskId(self, taskId):
        where_dict = dict(taskId=taskId)
        return self.db.select(self.table, where=web.db.sqlwhere(where_dict))

    def AddRecord(self, data):
        return self.db.insert(self.table, **data)

    def UpdateRecord(self, taskId, updateData):
        if not isinstance(updateData, dict):
            raise Exception("updateData必须是字典")
        return self.db.update(self.table, vars={'id': taskId}, where="taskId = $id", **updateData)
