# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from mainapp.config.db import DB, IDENTITY_DB
import web
import datetime
import json
import re


class Identity(object):
    """
    验证发送过来的验证码
    """
    def __init__(self):
        self.db = IDENTITY_DB

    def get_task_id(self, code):
        """
        根据验证码， 获取taskid
        """
        where_dict = dict(code=code)
        results = self.db.select("identity_code", where=web.db.sqlwhere(where_dict))
        if results:
            for r in results:
                create_time = r["create_time"]
                if (create_time + datetime.timedelta(hours=12)) > datetime.datetime.now():
                    info = {"success": True, "task_id": r["related_task_id"], "info": ""}
                    return info
                else:
                    continue
            info = {"success": False, "task_id": "", "info": "验证码: {} 已经超时， 请重新获取验证码".format(code)}
            return info
        else:
            info = {"success": False, "task_id": "", "info": "验证码: {} 错误！".format(code)}
            return info

    def add_download_record(self, data):
        return self.db.insert("download_info", **data)


class Download(object):
    """
    检索mysql的biocluster库，获取相关的信息， 为分析人员的现在任务文件做准备
    """
    def __init__(self):
        self.table = "workflow"
        self.db = DB

    def get_path_by_workflow_id(self, wid):
        """
        更具输入的任务id， 生成相对于workspace的相对路径
        """
        where_dict = dict(workflow_id=wid)
        result = self.db.select(self.table, where=web.db.sqlwhere(where_dict))
        if result:
            r = result[0]
            date = r["run_time"]
            dateStr = "{}{:0>2d}{:0>2d}".format(date.year, date.month, date.day)
            info = json.loads(r["json"])
            name = re.split("\.", info["name"]).pop(-1).split("_")
            name = "".join([i.capitalize() for i in name])
            path = "{}/{}_{}".format(dateStr, name, wid)
            print "相对路径生成完毕，为： {}".format(path)
            return path
        else:
            str_ = "在workflow表里未找到workflow_id为: {} 的记录".format(wid)
            print str_
            return "empty"

if __name__ == "__main__":
    d = Identity()
    d.get_task_id("ASDFGHJKL")
