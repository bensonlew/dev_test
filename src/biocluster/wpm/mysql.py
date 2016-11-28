# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import sys, MySQLdb, traceback
import time
from ..config import Config


class Mysql(object):
    def __init__(self,
                 host='',
                 user='',
                 passwd='',
                 db='',
                 port=3306,
                 charset='utf8'
                 ):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.charset = charset
        self.conn = None
        self._conn()
        self.cursor = None
        self.config = Config()

    def _conn(self):
        try:
            self.conn = MySQLdb.Connection(host=self.config.DB_HOST, user=self.config.DB_USER,
                                           passwd=self.config.DB_PASSWD, db=self.config.DB_NAME,
                                           port=self.config.DB_PORT, charset='utf8')
            return True
        except:
            return False

    def _re_conn(self, num=28800, stime=3):  # 重试连接总次数为1天,这里根据实际情况自己设置
        _number = 0
        _status = True
        while _status and _number <= num:
            try:
                self.conn.ping()  # cping 校验连接是否异常
                _status = False
            except:
                if self._conn() is True:  # 重新连接,成功退出
                    _status = False
                    break
                _number += 1
                time.sleep(stime)  # 连接不成功,休眠3秒钟,继续循环，知道成功或重试次数结束

    def select(self, sql=''):
        try:
            self._re_conn()
            self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            self.cursor.close()
            return result
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return False

    def select_limit(self, sql='', offset=0, length=20):
        sql = '%s limit %d , %d ;' % (sql, offset, length)
        return self.select(sql)

    def query(self, sql=''):
        try:
            self._re_conn()
            self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
            self.cursor.execute("set names utf8")  # utf8 字符集
            result = self.cursor.execute(sql)
            self.conn.commit()
            self.cursor.close()
            return result
        except MySQLdb.Error:
            return False

    def close(self):
        self.conn.close()
