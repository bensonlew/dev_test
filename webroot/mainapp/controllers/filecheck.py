# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.core.function import load_class_by_path
from mainapp.libs.signature import check_sig
from mainapp.libs.json_check import check_json
import web
import json


class FileCheck(object):

    @check_json
    @check_sig
    def POST(self):
        data = web.input()
        if not (hasattr(data, "format") and hasattr(data, "mod") and hasattr(data, "type") and hasattr(data, "path")):
            info = {"success": False, "info": u"参数不全"}
            return json.dumps(info)
        try:
            file_obj = load_class_by_path(data.format, "File")()
            file_obj.setpath(data.path)


        except Exception,e:
