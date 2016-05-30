# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.core.function import load_class_by_path
from biocluster.api.file.remote import RemoteFileManager
from mainapp.libs.signature import check_sig
import web
import json
from biocluster.core.exceptions import FileError
import os
# import traceback


class FileCheck(object):

    @check_sig
    def POST(self):
        data = web.input()
        for i in ["format", "module", "type", "path"]:
            if not hasattr(data, i):
                msg = {"success": False, "info": "参数不全"}
                return json.dumps(msg)
            if getattr(data, i).strip() == "":
                msg = {"success": False, "info": "参数%s不能为空" % i}
                return json.dumps(msg)
        return self.check(data)

    def check(self, data):
        try:
            file_obj = load_class_by_path(data.format, "File")()
            file_manager = RemoteFileManager(data.path)
            if file_manager.type != "local" and file_manager.type != "http":
                config = file_manager.config.get_netdata_config(file_manager.type)
                full_path = os.path.join(config[file_manager.type + "_path"], file_manager.path)
            else:
                full_path = data.path
            file_obj.set_path(full_path)
            paths = data.module.split(".")
            function_name = "_".join(paths)
            if data.type.lower() == "tool":
                function_name += "_tool_check"
            elif data.type.lower() == "module":
                function_name += "_module_check"
            elif data.type.lower() == "workflow":
                function_name += "_workflow_check"
            else:
                msg = {"success": False, "info": "参数type不能为" % data.type}
                return json.dumps(msg)
            if hasattr(data, "check") and data.check:
                if hasattr(file_obj, data.check):
                    getattr(file_obj, data.check)()
                else:
                    info = {"success": False, "info": "文件类%s中未定义指定的检测函数%s!" % (data.format, data.check)}
                    return json.dumps(info)
            else:
                if hasattr(file_obj, function_name):
                    getattr(file_obj, function_name)()
                else:
                    getattr(file_obj, "check")()
        except ImportError, e:
            info = {"success": False, "info": "文件模块错误: %s" % e}
            return json.dumps(info)
        except FileError, e:
            info = {"success": False, "info": "文件检测错误: %s" % e}
            return json.dumps(info)
        except Exception, e:
            # exstr = traceback.format_exc()
            # print exstr
            info = {"success": False, "info": "错误: %s" % e}
            return json.dumps(info)
        else:
            info = {"success": True, "info": "检测通过"}
            return json.dumps(info)


class TestData(object):
    def __init__(self):
        self.format = None
        self.module = None
        self.type = None
        self.path = None
        self.check = None


class MultiFileCheck(object):

    def __init__(self):
        self.checker = FileCheck()

    @check_sig
    def POST(self):
        data = web.input()
        print data
        print "checkflag_data"
        if not hasattr(data, "content"):
                msg = {"success": False, "info": "缺少参数content！"}
                return json.dumps(msg)
        try:
            json_obj = json.loads(data.content)
        except ValueError:
            info = {"success": False, "info": "content必须为Json格式!"}
            return json.dumps(info)
        else:
            print "checkflag_2"
            for i in ["module", "type", "files"]:
                if i not in json_obj.keys():
                    info = {"success": False, "info": "缺少参数:%s" % i}
                    return json.dumps(info)
            print "checkflag_3"
            if not isinstance(json_obj["files"], list) or len(json_obj["files"]) == 0:
                info = {"success": False, "info": "必须至少有一个检测文件!"}
                return json.dumps(info)
            message = {"success": True, "files": []}
            for f in json_obj["files"]:
                d = TestData()
                d .module = json_obj["module"]
                d .type = json_obj["type"]
                d .format = f["format"]
                d .path = f["path"]
                result = json.loads(self.checker.check(d))
                print "checkflag_result"
                print result
                if result["success"]:
                    x = {
                        "name": f["name"],
                        "pass": True
                    }
                else:
                    x = {
                        "name": f["name"],
                        "pass": False,
                        "info": result["info"]
                    }
                message["files"].append(x)
            return json.dumps(message)
