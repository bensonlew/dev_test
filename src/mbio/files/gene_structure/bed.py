# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.iofile import File
from biocluster.core.exceptions import FileError
# import subprocess


class BedFile(File):
    """
    定义比对结果sam格式文件
    """

    def __init__(self):
        super(BedFile, self).__init__()

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(BedFile, self).check():
            return True
        else:
            raise FileError("文件格式错误")

    def get_info(self):
        """
        获取文件属性
        :return:
        """
        super(BedFile, self).get_info()
        self.get_bed_info()

    def get_bed_info(self):
        with open(self.prop['path'], 'r') as f:
            f.readline()
            row_num = len(f.next().strip().split())
            if row_num != 12:
                return False
