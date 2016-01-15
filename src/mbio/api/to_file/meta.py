# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import os


def export_otu_table(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "%s_input.json" % option_name)
    bind_obj.logger.debug("正在到处参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))

    return file_path
