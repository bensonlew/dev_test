# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.config.db import DB


class ClientKey(object):
    def __init__(self, client):
        self.db = DB
        self.client = client
        self.table = "clientkey"

    def get_key(self):
        myvar = dict(client=self.client)
        return self.db.select(self.table, myvar, where="client = $client")[0].key
