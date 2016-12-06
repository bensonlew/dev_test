# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
# from mainapp.libs.signature import check_sig
from gevent import monkey; monkey.patch_all()
import web
import json
import tempfile
from gevent.subprocess import Popen


class DownloadWebPic(object):
    # @check_sig
    def POST(self, my_type):
        data = web.input()
        print('Convert Type: {}'.format(my_type))
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
        if not data.scale.isalnum():
            msg = {"success": False, "info": "scale参数必须是数值:{}".format(data.scale)}
            return json.dumps(msg)
        file_pic = self._svg_convert()
        if file_pic:
            web.header('Content-Type', 'application/octet-stream')
            web.header('Transfer-Encoding', 'chunked')
            web.header('Content-disposition', 'attachment; filename={}'.format(data.file_name))
            return open(file_pic, 'rb').read()
        else:
            msg = {"success": False, "info": "生成结果文件出错".format(data.scale)}
            return json.dumps(msg)

    def _svg_convert(self):
        temp_dir = tempfile.mkdtemp()
        temp_svg = temp_dir + '/temp.svg'
        with open(temp_svg, 'wb') as w:
            w.write(web.input().svg_data)
        temp_pic = temp_dir + '/temp.' + web.input().file_type
        cmd = 'cairosvg {} -f {} -o {} -s {}'.format(temp_svg, web.input().file_type, temp_pic, int(web.input().scale))
        pro = Popen(cmd, shell=True)
        pro.wait()
        if pro.returncode == 0:
            print("TEMP PIC: {}".format(temp_pic))
            return temp_pic
        else:
            return False



if __name__ == '__main__':
    print tempfile.mkdtemp()
    a = Popen("dir ./fsa", shell=True)
    a.wait()
    print a.returncode
    print 'hhhhhhhhhhhhhhhhhhhh'
