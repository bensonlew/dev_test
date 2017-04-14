# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from mainapp.config.db import Config
import web


class ClientKey(object):
    def __init__(self, client):
        self._db = None
        self.client = client
        self.table = "clientkey"
        self.key = None
        self.ipmask = None
        self.timelimit = None
        self._select()

    @property
    def db(self):
        if self._db:
            return self._db
        else:
            self._db = Config().get_db()
            return self._db

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
