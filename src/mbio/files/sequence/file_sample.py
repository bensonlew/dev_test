# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class FileSampleFile(File):
    """
    定义 文件名——样本 的文件格式
    这里的名称是文件名，当qc模块的输入是一个fastq文件夹的时候，用于规范化文件名
    """
    def __init__(self):
        super(FileSampleFile, self).__init__()
        self.col = 0
        self.repeat_name = False

    def get_info(self):
        """
        获取文件属性
        """
        super(FileSampleFile, self).get_info()
        (sample, name) = self.get_file_info()
        self.set_property("sample_number", len(sample))
        self.set_property("file_number", len(name))
        self.set_property("file_names", name.keys())
        self.set_property("sample_names", sample.keys())

    def get_file_info(self):
        """
        获取file_sample文件的信息
        """
        with open(self.prop['path'], 'r') as f:
            sample = dict()
            name = dict()
            for line in f:
                line = line.rstrip('\n')
                line = re.split('\t', line)
                self.col = len(line)
                if line[1] not in sample.keys():
                    sample[line[1]] = 1
                if line[0] not in name.keys():
                    name[line[0]] = 1
                else:
                    self.repeat_name = True
        return sample, name

    def check(self):
        if super(FileSampleFile, self).check():
            if self.prop["sample_number"] == 0:
                raise FileError('应该至少包含一个样本')
            if self.col != 2:
                raise FileError('这个文件的列数为2')
            if self.repeat_name:
                raise FileError('文件名不能重复！')
            return True
