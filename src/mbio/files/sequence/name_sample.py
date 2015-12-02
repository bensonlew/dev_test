# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class NameSampleFile(File):
    """
    定义 名称——样本 的文件格式
    这里的名称可以是文件名，当qc模块的输入是一个fastq文件夹的时候，用于规范化文件名
    这里的名称也可以是序列名，当qc模块的输入是一个fastq文件的时候，用于拆分出各个样本
    """
    def __init__(self):
        super(NameSampleFile, self).__init__()
        self.col = 0
        self.repeat_name = False

    def get_info(self):
        """
        获取文件属性
        """
        super(NameSampleFile, self).get_info()
        (info, count) = self.get_file_info()
        self.set_property("sample_number", len(info))
        self.set_property("seq_number", count)
        self.set_property("sample_names", info.keys())

    def get_file_info(self):
        """
        获取name_sample文件的信息
        """
        with open(self.prop['path'], 'r') as f:
            sample = dict()
            name = dict()
            count = 0
            for line in f:
                count += 1
                line = line.rstrip('\n')
                line = re.split('\t', line)
                self.col = len(line)
                if line[1] not in sample.keys():
                    sample[line[1]] = 1
                if line[0] not in name.keys():
                    name[line[0]] = 1
                else:
                    self.repeat_name = True
        return sample, count

    def check(self):
        if super(NameSampleFile, self).check():
            if self.prop["sample_number"] == 0:
                raise FileError('应该至少包含一个样本')
            if self.col != 2:
                raise FileError('这个文件的列数为2')
            if self.repeat_name:
                raise FileError('文件名/序列名不能重复！')
            return True
