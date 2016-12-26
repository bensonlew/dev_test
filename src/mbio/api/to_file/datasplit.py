# -*- coding: utf-8 -*-
# __author__ = 'xuting'

import os
import json


def export_sample_info(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "%s.info_json" % option_name)
    bind_obj.logger.debug("正在导出参数%s的info文件，路径:%s" % (option_name, file_path))
    with open(file_path, "wb") as w:
        json.dump(data, w)
    return file_path
