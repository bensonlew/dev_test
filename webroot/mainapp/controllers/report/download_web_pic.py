# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from mainapp.libs.signature import check_sig
import web
import json
import os
import traceback


class DownloadWebPic(object):
    @check_sig
    def POST(self):
        data = web.input()
        print "******"
        print data
        print "******"
        for i in ["file_type", "file_name", "svg_data", "svg_width", "svg_height", "scale"]:
            if not hasattr(data, i):
                msg = {"success": False, "info": "缺少参数: {}".format(i)}
                return json.dumps(msg)
            if getattr(data, i).strip() == "":
                msg = {"success": False, "info": "参数%s不能为空" % i}
                return json.dumps(msg)
        web.header('Content-Type', 'application/octet-stream')
        web.header('Transfer-Encoding', 'chunked')
        web.header('Content-disposition', 'attachment; filename=your.file')
        
        return self.check(data)
