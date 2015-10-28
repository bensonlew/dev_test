# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
import os
import ConfigParser


CONF_FILE = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../src/biocluster/main.conf"))


def get_db():
    rcf = ConfigParser.RawConfigParser()
    rcf.read(CONF_FILE)
    DB_TYPE = rcf.get("DB", "dbtype")
    DB_HOST = rcf.get("DB", "host")
    DB_USER = rcf.get("DB", "user")
    DB_PASSWD = rcf.get("DB", "passwd")
    DB_NAME = rcf.get("DB", "db")
    DB_PORT = rcf.get("DB", "port")

    if DB_TYPE == "mysql":
        return web.database(dbn=DB_TYPE, host=DB_HOST, db=DB_NAME, user=DB_USER, passwd=DB_PASSWD, port=int(DB_PORT))

DB = get_db()
