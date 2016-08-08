# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
import os
import ConfigParser
from pymongo import MongoClient
from biocluster.core.singleton import singleton


@singleton
class Config(object):
    def __init__(self):
        self.CONF_FILE = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../src/biocluster/main.conf"))
        self.rcf = ConfigParser.RawConfigParser()
        self.rcf.read(self.CONF_FILE)
        self._db = None
        self._identity_db = None
        self._mongo_client = None

    def get_db(self):
        if not self._db:
            dbtype = self.rcf.get("DB", "dbtype")
            host = self.rcf.get("DB", "host")
            user = self.rcf.get("DB", "user")
            passwd = self.rcf.get("DB", "passwd")
            dbname = self.rcf.get("DB", "db")
            port = self.rcf.get("DB", "port")
            self._db = web.database(dbn=dbtype, host=host, db=dbname, user=user, passwd=passwd, port=int(port))
        return self._db

    def get_identity_db(self):
        if not self._identity_db:
            dbtype = self.rcf.get("IDENTITY_DB", "dbtype")
            host = self.rcf.get("IDENTITY_DB", "host")
            user = self.rcf.get("IDENTITY_DB", "user")
            passwd = self.rcf.get("IDENTITY_DB", "passwd")
            dbname = self.rcf.get("IDENTITY_DB", "db")
            port = self.rcf.get("IDENTITY_DB", "port")
            self._identity_db = web.database(dbn=dbtype, host=host, db=dbname, user=user, passwd=passwd, port=int(port))
        return self._identity_db

    def get_mongo_client(self):
        if not self._mongo_client:
            uri = Config().rcf.get("MONGO", "uri")
            self._mongo_client = MongoClient(uri)
        return self._mongo_client

    def get_work_dir(self):
        return self.rcf.get("Basic", "work_dir")


def get_db():
    return Config().get_db()

DB = get_db()
IDENTITY_DB = Config().get_identity_db()


def get_use_api_clients():
    return Config().rcf.options("API")


def get_api_type(client):
    if Config().rcf.has_option("API", client):
        return Config().rcf.get("API", client)
    else:
        return None


def get_mongo_client():
    return Config().get_mongo_client()
