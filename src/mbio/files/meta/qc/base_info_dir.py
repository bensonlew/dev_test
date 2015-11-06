# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
import os
from biocluster.core.exceptions import FileError
from biocluster.iofile import Directory


class BaseInfoDir(Directory):
    """
    定义base_info文件夹
    """
    def __init__(self):
        super(BaseInfoDir, self).__init__()

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' in self.prop.keys() and os.path.isdir(self.prop['path']):
            self.set_property("base_info_number", self.get_base_info_number())
        else:
            raise FileError(u"文件夹路径不正确，请设置正确的文件夹路径!")

    def get_base_info_number(self):
        """
        获取文件夹下base_info文件的数目
        """
        filelist = os.listdir(self.prop['path'])
        count = 0
        for file_ in filelist:
            if re.search(r'\.base_info$', file_):
                count += 1
        return count

    def check(self):
        """
        检测文件夹是否满足要求，不满足是触发FileError异常
        """
        if super(BaseInfoDir, self).check():
            if "file_number" not in self.prop.keys():
                raise FileError(u"还未设置该文件夹下的base_info文件数目")
            if self.prop['file_number'] != self.get_fastq_number():
                raise FileError(u"实际base_info文件数目不等于设定值")
