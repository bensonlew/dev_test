#!/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.config import Config
from multiprocessing import Process
from biocluster.api.file.remote import RemoteFileManager
import os
import sys
import datetime
import time
import json
import traceback
import gevent
from biocluster.core.function import hostname, daemonize
import atexit


def delpid():
    pid_file = Config().SERVICE_PID
    pid_file = pid_file.replace('$HOSTNAME', hostname + ".upload")
    os.remove(pid_file)


def writepid():
    pid = str(os.getpid())
    pid_file = Config().SERVICE_PID
    pid_file = pid_file.replace('$HOSTNAME', hostname + ".upload")
    if not os.path.exists(os.path.dirname(pid_file)):
        os.mkdir(os.path.dirname(pid_file))
    with open(pid_file, 'w+') as f:
        f.write('%s\n' % pid)
    atexit.register(delpid)


class UploadManager(object):
    def __init__(self):
        self._running = {}
        self._db = None

    @property
    def db(self):
        if not self._db:
            self._db = Config().get_db()
        return self._db

    def wacth_as_service(self):
        timestr = ""
        while True:

            if timestr != time.strftime('%Y%m%d', time.localtime(time.time())):
                timestr = time.strftime('%Y%m%d', time.localtime(time.time()))
                log = self.get_log_path()
                so = file(log, 'a+')
                se = file(log, 'a+', 0)
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())
                self.log("开始监控需要上传的文件")

            tasks = self.get_upload_tasks()
            if tasks:
                for record in tasks:
                    if record.id not in self._running.keys():
                        try:
                            process = Uploader(record)
                            process.start()
                        except Exception, e:
                            exstr = traceback.format_exc()
                            print exstr
                            self.log("运行上传任务出错: %s" % e)
                        else:
                            self._running[record.id] = process

            for key in self._running.keys():
                p = self._running[key]
                if not p.is_alive():
                    exitcode = p.exitcode
                    if exitcode != 0:
                        self.log("上传任务%s出现异常" % p.id)
                    p.join()
                    del self._running[key]
            gevent.sleep(Config().UPDATE_FREQUENCY)

    @staticmethod
    def log(info):
        print("%s\t%s" % (datetime.datetime.now(), info))
        sys.stdout.flush()

    @staticmethod
    def get_log_path():
        timestr = time.strftime('%Y%m%d', time.localtime(time.time()))
        if not os.path.exists(Config().UPLOAD_LOG):
            os.mkdir(Config().UPLOAD_LOG)
        log_path = os.path.join(Config().UPLOAD_LOG, "%s.log" % timestr)
        return log_path

    def get_upload_tasks(self):
        sql = "SELECT id,task_id,upload,has_upload,uploaded from apilog where " \
              "(success=0 and reject=0 and has_upload=0) or (success=0 and reject=0 and has_upload=1 and uploaded=1)"

        results = self.db.query(sql)
        return results


class Uploader(Process):

    def __init__(self, record):
        super(Uploader, self).__init__()
        self.id = record.id
        self._db = ""
        self._record = record

    @property
    def db(self):
        if not self._db:
            self._db = Config().get_db()
        return self._db

    def upload(self):
        log = self.get_log_path()
        so = file(log, 'a+')
        se = file(log, 'a+', 0)
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        up_data = json.loads(self._record.upload)
        remote_file = RemoteFileManager(up_data["target"])
        for sub_dir in up_data["files"]:
            self.log("开始上传%s到%s" % (sub_dir, remote_file))
            if remote_file.type != "local":
                umask = os.umask(0)
                remote_file.upload(sub_dir)
                # for root, subdirs, files in os.walk("c:\\test"):
                #     for filepath in files:
                #         os.chmod(os.path.join(root, filepath), 0o777)
                #     for sub in subdirs:
                #         os.chmod(os.path.join(root, sub), 0o666)
                os.umask(umask)
            self.log("上传%s到%s完成" % (sub_dir, remote_file))

    def log(self, info):
        print("%s\tid:%s\ttask_id:%s\t%s" % (datetime.datetime.now(), self._record.id, self._record.task_id, info))
        sys.stdout.flush()

    @staticmethod
    def get_log_path():
        timestr = time.strftime('%Y%m%d', time.localtime(time.time()))
        if not os.path.exists(Config().UPLOAD_LOG):
            os.mkdir(Config().UPLOAD_LOG)
        log_path = os.path.join(Config().UPLOAD_LOG, "%s.log" % timestr)
        return log_path

    def run(self):
        super(Uploader, self).run()
        try:
            self.upload()
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            self.log("上传出错: %s" % e)
        else:
            self.db.update("apilog", vars={"id": self.id}, where="id = $id", uploaded=1)
            self.log("上传全部完成")


def main():
    um = UploadManager()
    pid_file = Config().SERVICE_PID
    pid_file = pid_file.replace('$HOSTNAME', hostname + ".upload")
    if os.path.isfile(pid_file):
        raise Exception("PID file already exists,if this service already running?")
    daemonize()
    writepid()
    um.wacth_as_service()

if __name__ == "__main__":
    main()