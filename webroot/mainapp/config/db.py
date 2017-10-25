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
        self._biodb_mongo_client = None
        self._record_db = None

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

    def get_identity_db(self, test=False):
        """
        上传、下载验证码数据库
        """
        if test:
            identity_db = 'T_IDENTITY_DB'
        else:
            identity_db = 'IDENTITY_DB'
        # if not self._identity_db:
        dbtype = self.rcf.get(identity_db, "dbtype")
        host = self.rcf.get(identity_db, "host")
        user = self.rcf.get(identity_db, "user")
        passwd = self.rcf.get(identity_db, "passwd")
        dbname = self.rcf.get(identity_db, "db")
        port = self.rcf.get(identity_db, "port")
        self._identity_db = web.database(dbn=dbtype, host=host, db=dbname, user=user, passwd=passwd, port=int(port))
        return self._identity_db

    def get_record_db(self):
        """
        上传、下载记录数据库
        """
        if not self._record_db:
            dbtype = self.rcf.get("DATA_RECORD_DB", "dbtype")
            host = self.rcf.get("DATA_RECORD_DB", "host")
            user = self.rcf.get("DATA_RECORD_DB", "user")
            passwd = self.rcf.get("DATA_RECORD_DB", "passwd")
            dbname = self.rcf.get("DATA_RECORD_DB", "db")
            port = self.rcf.get("DATA_RECORD_DB", "port")
            self._record_db = web.database(dbn=dbtype, host=host, db=dbname, user=user, passwd=passwd, port=int(port))
        return self._record_db

    def get_mongo_client(self, mtype=None, ref=False):
        if ref:
            if not self._biodb_mongo_client:
                if mtype:
                    if self.rcf.has_option("MONGO", "%s_ref_uri" % mtype):
                        uri = self.rcf.get("MONGO", "%s_ref_uri" % mtype)
                    elif self.rcf.has_option("MONGO", "%s_uri" % mtype):
                        uri = self.rcf.get("MONGO", "%s_uri" % mtype)
                    else:
                        uri = self.rcf.get("MONGO", "bio_uri")
                else:
                    uri = self.rcf.get("MONGO", "bio_uri")
                self._biodb_mongo_client = MongoClient(uri, connect=False, maxPoolSize=1000)
            return self._biodb_mongo_client
        else:
            if not self._mongo_client:
                if mtype:
                    if self.rcf.has_option("MONGO", "%s_uri" % mtype):
                        uri = self.rcf.get("MONGO", "%s_uri" % mtype)
                    else:
                        uri = self.rcf.get("MONGO", "uri")
                else:
                    uri = self.rcf.get("MONGO", "uri")
                self._mongo_client = MongoClient(uri, connect=False, maxPoolSize=1000)
            return self._mongo_client

    def get_mongo_dbname(self, mtype=None, ref=False):
        if not mtype:
            return self.MONGODB
        else:
            key = "%s_db_name" % mtype
            if ref:
                key = "%s_ref_db_name" % mtype
            return self.rcf.get("MONGO", key)

    def get_biodb_mongo_client(self, mtype=None):
        return self.get_mongo_client(mtype, True)

    def get_work_dir(self):
        return self.rcf.get("Basic", "work_dir")


def get_db():
    return Config().get_db()


def get_use_api_clients():
    return Config().rcf.options("API")


def get_api_type(client):
    if Config().rcf.has_option("API", client):
        return Config().rcf.get("API", client)
    else:
        return None


def get_mongo_client(mtype=None,ref=False):
    return Config().get_mongo_client(mtype,ref)
