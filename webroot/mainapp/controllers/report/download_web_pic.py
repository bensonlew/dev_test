# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
# from mainapp.libs.signature import check_sig
import web
import json
import os
import tempfile
import traceback
from gevent.subprocess import Popen


class DownloadWebPic(object):
    # @check_sig
    def POST(self):
        data = web.input()
        print "******"
        print data
        print "******"
        for i in ["file_type", "file_name", "svg_data", "scale"]:
            if not hasattr(data, i):
                msg = {"success": False, "info": "缺少参数: {}".format(i)}
                return json.dumps(msg)
            if getattr(data, i).strip() == "":
                msg = {"success": False, "info": "参数%s不能为空" % i}
                return json.dumps(msg)
        if data.file_type not in ['png', 'pdf']:
            msg = {"success": False, "info": "结果文件类型必须为png或者pdf:{}".format(data.file_type)}
            return json.dumps(msg)
        web.header('Content-Type', 'application/octet-stream')
        web.header('Transfer-Encoding', 'chunked')
        web.header('Content-disposition', 'attachment; filename=your.file')

        return self.check(data)

    def _svg_convert(self):
        temp_dir = tempfile.mkdtemp()
        temp_svg = temp_dir + '/temp.svg'
        with open(temp_svg, 'wb') as w:
            w.write(web.input().svg_data)
        temp_pic = temp_dir + '/temp.' + web.input().file_type


if __name__ == '__main__':
    print tempfile.mkdtemp()
    a = Popen("dir ./fsa", shell=True)
    a.wait()
    print a.returncode
    print 'hhhhhhhhhhhhhhhhhhhh'
