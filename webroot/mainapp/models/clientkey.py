# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from mainapp.config.db import DB
import web


class ClientKey(object):
    def __init__(self, client):
        self.db = DB
        self.client = client
        self.table = "clientkey"
        self.key = None
        self.ipmask = None
        self.timelimit = None
        self._select()

    def _select(self):
        myvar = dict(client=self.client)
        data = self.db.select(self.table, myvar, where="client = $client")
        if len(data) < 1:
            raise web.badrequest("client not found!")
        else:
            record = data[0]
            self.key = record.key
            self.ipmask = record.ipmask
            self.timelimit = record.timelimit
