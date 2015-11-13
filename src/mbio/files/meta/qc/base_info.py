# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class BaseInfoFile(File):
    """
    定义BaseInfoFile(meta里的单样本碱基质量)文件
    """
    def __init__(self):
        super(BaseInfoFile, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(BaseInfoFile, self).get_info()
        file_info = self.get_file_info()
        self.set_property("longest_cycle", file_info)

    def check(self):
        """
        检测文件是否满足要求,发生错误时应该触发FileError异常
        :return: bool
        """
        if super(BaseInfoFile, self).check():
            if not self.check_format:
                raise FileError("文件格式错误")
        return True

    def get_file_info(self):
        """
        获取base_info文件的信息
        :param row:行数
        """
        row = 0
        with open(self.prop['path'], 'r') as f:
            if f.readline():
                row += 1
        return row - 1

    @property
    def check_format(self):
        """
        检测文件头是否符合要求
        """
        row = 0
        head = ["column", "count", "min", "max", "mean", "Q1", "med", "Q3"]
        with open(self.prop['path'], 'r') as f:
            line = f.readline()
            row += 1
            line = re.split(r'\s+', line)
            if row == 1:
                format_ = True
                for col in head:
                    if col not in line:
                        format_ = False
                return format_
