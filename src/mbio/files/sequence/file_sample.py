# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
import os
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
        self.file_sample = dict()  # 文件名与样本名的对应

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
        self.set_property("file_sample", self.file_sample)

    def get_file_info(self):
        """
        获取file_sample文件的信息
        """
        dir_name = os.path.dirname(self.prop['path'])
        with open(self.prop['path'], 'r') as f:
            sample = dict()
            name = dict()
            for line in f:
                if "#" in line:
                    continue
                line = line.rstrip('\n')
                line = re.split('\t', line)
                self.col = len(line)
                if line[1] not in sample.keys():
                    sample[line[1]] = 1
                if line[0] not in name.keys():
                    name[line[0]] = 1
                else:
                    self.repeat_name = True
                full_name = os.path.join(dir_name, line[0])
                if os.path.isfile(full_name):
                    self.file_sample[line[0]] = line[1]
        return sample, name

    def check_exists(self):
        """
        检查file_list中的每个文件是否都存在
        """
        dir_name = os.path.dirname(self.prop['path'])
        with open(self.prop['path'], 'r') as f:
            for line in f:
                if "#" in line:
                    continue
                line = line.rstrip().split("\t")
                full_name = os.path.join(dir_name, line[0])
                if not os.path.isfile(full_name):
                    raise FileError("文件{}不存在".format(full_name))
        return True

    def check(self):
        if super(FileSampleFile, self).check():
            self.get_info()
            if self.prop["sample_number"] == 0:
                raise FileError('应该至少包含一个样本')
            if self.col != 2:
                raise FileError('这个文件的列数应该为2')
            if self.repeat_name:
                raise FileError('文件名不能重复！')
            self.check_exists()
            return True
