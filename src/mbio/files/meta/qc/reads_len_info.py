# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class ReadsLenInfo(File):
    """
    定义ReadsLenInfo(meta里reads长度分布)文件
    """
    def __init__(self):
        super(ReadsLenInfo, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(ReadsLenInfo, self).get_info()
        sample_number = self.get_sample_number()
        step = self.get_step()
        self.set_property("sample_number", sample_number)
        self.set_property("step", step)

    def check(self):
        """
        检测文件是否满足要求,发生错误时应该触发FileError异常
        :return: bool
        """
        if super(ReadsLenInfo, self).check():
            if not self.check_file_name:
                raise FileError(u"文件名错误")
            if not self.check_format:
                raise FileError(u"文件格式错误")
        return True

    def get_sample_number(self):
        """
        获取reads_len_info文件的样本个数
        :param row:行数
        """
        row = 0
        with open(self.prop['path'], 'r'):
            row += 1
        return row - 1

    def get_step(self):
        """
        根据文件名获取reads_len_info文件的步长
        """
        (str_, step) = re.split(r'-', self.prop['basename'])
        return step

    @property
    def check_file_name(self):
        """
        检查文件名是否按照要求
        """
        step_list = ["1", "20", "50", "100"]
        (str_, step) = re.split(r'-', self.prop['basename'])
        if str_ != "step":
            return False
        if step not in step_list:
            return False
        return True

    @property
    def check_format(self):
        """
        检测文件头是否符合要求
        """
        row = 0
        with open(self.prop['path'], 'r') as f:
            line = f.readline()
            row += 1
            line = re.split(r'\s+', line)
            if row == 1:
                for i in range(1, len(line)):
                    (start, end) = re.split(r'-', line[i])
                    if end - start != self.prop['step'] + 1:
                        return False
                return True
