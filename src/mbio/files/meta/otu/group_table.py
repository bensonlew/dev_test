# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class GroupTableFile(File):
    """
    定义group_table格式文件
    """
    def __init__(self):
        super(GroupTableFile, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(GroupTableFile, self).get_info()
        info = self.get_file_info()
        self.set_property("sample_number", len(info[0]))
        self.set_property("sample", info[0])
        self.set_property("group", info[1])
        self.set_property("group_number", len(info[1]))

    def get_file_info(self):
        """
        获取group_table文件的信息
        """
        row = 0
        with open(self.prop['path'], 'r') as f:
            sample = dict()
            group = dict()
            line = f.readline()
            line = re.split("\s+", line)
            row += 1
            if row > 1:
                if line[0] not in sample.keys():
                    sample[line[0]] = 1
                if line[1] not in group.keys():
                    group[line[0]] = 1
            return (sample, group)

    def check(self):
        if super(GroupTableFile, self).check():
            if self.prop['sample_number'] == 0:
                raise FileError(u'应该至少包含一个样本')
            if self.prop['group_number'] == 0:
                raise FileError(u'应该至少包含一个分组')
