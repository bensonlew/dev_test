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
from biocluster.wsheet import Sheet
from biocluster.api.database.base import ApiManager
from biocluster.logger import Wlog
import urlparse
from biocluster.core.function import CJsonEncoder
import urllib
import argparse

parser = argparse.ArgumentParser(description="upload file or import data")
group = parser.add_mutually_exclusive_group()
group.add_argument("-s", "--service",  help="service mode")
group.add_argument("-i", "--api_log_id", help="upload data for the give api log id")

args = parser.parse_args()


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

    def upload(self, id):
        record = self.get_upload_task(id)
        if record:
            process = UploadProcess(record, debug=True)
            process.start()
        else:
            print "记录%s不存在或者不能上传" % id

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
                            process = UploadProcess(record)
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
        sql = "SELECT id,task_id,api,`data`,run_time,upload,has_upload,uploaded from apilog where" \
              " success=0 and reject=0 and has_upload=1 and uploaded=0)"

        results = self.db.query(sql)
        if isinstance(results, long) or isinstance(results, int):
            return None
        if len(results) > 0:
            return results
        else:
            return None

    def get_upload_task(self, id):
        sql = "SELECT id,task_id,api,`data`,run_time,upload,has_upload,uploaded from apilog where" \
              " success=0 and reject=0 and has_upload=1 and uploaded=0 and id=%s) % id"

        results = self.db.query(sql)
        if isinstance(results, long) or isinstance(results, int):
            return None
        if len(results) > 0:
            return results[0]
        else:
            return None


class BindObject(object):
    def __init__(self, record):
        self.record = record
        self.id = ""
        self.name = ""
        self.work_dir = ""
        self.fullname = ""
        self.output_dir = ""
        self._sheet = None
        self._data = self._load()
        self._logger = Wlog(self).get_logger("")

    def _load(self):
        data = json.loads(self.record.upload)
        self.id = data["bind"]["id"]
        self.name = data["bind"]["name"]
        self.work_dir = data["bind"]["workdir"]
        self.fullname = data["bind"]["fullname"]
        self.output_dir = data["bind"]["output"]
        return data

    @property
    def sheet(self):
        if "sheet" in self._data["bind"]:
            if not self._sheet:
                self._sheet = Sheet(data=self._data["bind"]["sheet"])
        return self._sheet

    @property
    def logger(self):
        return self._logger


class ReportUploader(object):
    def __init__(self, record, bind_object, debug=False):
        self._record = record
        self._bind_object = bind_object
        self._debug = debug

    def api_replay(self):
        api_manager = ApiManager(self._bind_object, play_mod=True, debug=self._debug)
        api_manager.load_call_records_list(self._record.upload["call"])
        api_manager.play()


class Uploader(object):

    def __init__(self, record, bind_object):
        super(Uploader, self).__init__()
        self.id = record.id
        self._db = ""
        self._record = record
        self._bind_object = bind_object

    @property
    def db(self):
        if not self._db:
            self._db = Config().get_db()
        return self._db

    def upload(self):
        up_data = json.loads(self._record.upload)
        remote_file = RemoteFileManager(up_data["target"])
        for sub_dir in up_data["files"]:
            self._bind_object.logger.info("开始上传%s到%s" % (sub_dir, remote_file))
            if remote_file.type != "local":
                umask = os.umask(0)
                remote_file.upload(sub_dir)
                # for root, subdirs, files in os.walk("c:\\test"):
                #     for filepath in files:
                #         os.chmod(os.path.join(root, filepath), 0o777)
                #     for sub in subdirs:
                #         os.chmod(os.path.join(root, sub), 0o666)
                os.umask(umask)
            self._bind_object.logger.info("上传%s到%s完成" % (sub_dir, remote_file))


class UploadProcess(Process):
    def __init__(self, record, debug=False):
        super(UploadProcess, self).__init__()
        self._record = record
        self._bind_object = BindObject(record)
        self._debug = debug
        self.report_error = None
        self.file_error = None
        self._db = None
        self._statu_data = self.load_statu_data()

    @property
    def db(self):
        if not self._db:
            self._db = Config().get_db()
        return self._db

    def run(self):
        super(UploadProcess, self).run()
        if not self._debug:
                timestr = time.strftime('%Y%m%d', time.localtime(time.time()))
                log_dir = os.path.join(Config().SERVICE_LOG, timestr)
                if not os.path.exists(log_dir):
                    os.mkdir(log_dir)
                if self._bind_object.sheet:
                    my_id = self._bind_object.sheet.id
                else:
                    my_id = self._record.task_id
                log = os.path.join(log_dir, "%s.log" % my_id)
                so = file(log, 'a+')
                se = file(log, 'a+', 0)
                os.dup2(so.fileno(), sys.stdout.fileno())
                os.dup2(se.fileno(), sys.stderr.fileno())

        try:
            file_uploader = Uploader(self._record, self._bind_object)
            file_uploader.upload()
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            self._bind_object.logger.error("上传文件出错: %s" % e)
            self.file_error = "上传文件出错: %s" % e

        try:
            report = ReportUploader(self._record, self._bind_object, debug=self._debug)
            report.api_replay()
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            self._bind_object.logger.error("导入数据库出错: %s" % e)
            self.report_error = "导入数据库出错: %s" % e

        if self.report_error is None and self.file_error is None:
            self.db.update("apilog", vars={"id": self._record.id}, where="id = $id", uploaded=1)
            self._bind_object.logger.info("上传全部完成")
        else:
            error = "%s  %s" % (self.report_error, self.report_error)
            self.new_failed_statu(error)

    def load_statu_data(self):
        url_data = urlparse.parse_qs(self._record.data)
        statu = url_data["content"][0]
        return json.loads(statu, object_hook=date_hook)

    def new_failed_statu(self, error):
        if "stage" in self._statu_data.keys():
            self._statu_data["stage"]["status"] = "failed"
            self._statu_data["stage"]["error"] = error
            self._statu_data["stage"]["created_ts"] = datetime.datetime.now()
            post_data = {
                "content": json.dumps(self._statu_data, cls=CJsonEncoder)
            }
            if self.file_error:
                myvar = dict(id=self._record.id)
                self.db.update("apilog", vars=myvar, where="id = $id", reject=1, response=self.file_error)
                post_data["file_upload_error"] = 1
            if self.report_error:
                post_data["import_date_error"] = 1
            data = {
                "task_id": self._record.task_id,
                "api": self._record.api,
                "data": urllib.urlencode(post_data)
            }
            self.db.insert("apilog", **data)


def date_hook(json_dict):
    for (key, value) in json_dict.items():
        try:
            json_dict[key] = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except:
            pass
    return json_dict


def main():
    um = UploadManager()
    if args.service:
        pid_file = Config().SERVICE_PID
        pid_file = pid_file.replace('$HOSTNAME', hostname + ".upload")
        if os.path.isfile(pid_file):
            raise Exception("PID file already exists,if this service already running?")
        daemonize()
        writepid()
        um.wacth_as_service()
    if args.api_log_id:
        um.upload()


if __name__ == "__main__":
    main()