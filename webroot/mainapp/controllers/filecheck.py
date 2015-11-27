# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.core.function import load_class_by_path
from mainapp.libs.signature import check_sig
import web
import json
from biocluster.core.exceptions import FileError


class FileCheck(object):

    @check_sig
    def POST(self):
        data = web.input()
        for i in ["format", "module", "type", "path"]:
            if not hasattr(data, i):
                msg = {"success": False, "info": u"参数不全"}
                return json.dumps(msg)
            if getattr(data, i).strip() == "":
                msg = {"success": False, "info": u"参数%s不能为空" % i}
                return json.dumps(msg)
        try:
            file_obj = load_class_by_path(data.format, "File")()
            file_obj.setpath(data.path)
            paths = data.module.split(".")
            function_name = "_".join(paths)
            if data.type.lower() == "tool":
                function_name += "_tool_check"
            elif data.type.lower() == "module":
                function_name += "_module_check"
            elif data.type.lower() == "workflow":
                function_name += "_workflow_check"
            else:
                msg = {"success": False, "info": u"参数type不能为" % data.type}
                return json.dumps(msg)
            if hasattr(data, "check") and data.check.strip() != "":
                if hasattr(file_obj, data.check):
                    getattr(file_obj, data.check)()
                else:
                    raise Exception("文件类%s中未定义指定的检测函数%s!" %
                                    (data.format, data.check))
            else:
                if hasattr(file_obj, function_name):
                    getattr(file_obj, function_name)()
                else:
                    getattr(file_obj, "check")()
        except ImportError, e:
            info = {"success": False, "info": u"文件模块错误: %s" % e}
            return json.dumps(info)
        except FileError, e:
            info = {"success": False, "info": u"文件检测错误: %s" % e}
            return json.dumps(info)
        except Exception, e:
            info = {"success": False, "info": u"错误: %s" % e}
            return json.dumps(info)
        else:
            info = {"success": True, "info": u"检测通过"}
            return json.dumps(info)
