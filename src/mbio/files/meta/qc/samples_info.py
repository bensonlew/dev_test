# -*- coding: utf-8 -*-
# __author__ = 'xuting'
from __future__ import division
import re
from decimal import Decimal
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class SamplesInfoFile(File):
    """
    定义samplesInfo(meta里的单样本碱基质量)文件
    """
    def __init__(self):
        super(SamplesInfoFile, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(SamplesInfoFile, self).get_info()
        file_info = self.get_file_info()
        self.set_property("sample_number", file_info[0])
        self.set_property("total_reads", file_info[1])
        self.set_property("total_bases", file_info[2])
        self.set_property("avg_length", file_info[3])
        self.set_property("min_length", file_info[4])
        self.set_property("max_length", file_info[5])

    def check(self):
        """
        检测文件是否满足要求,发生错误时应该触发FileError异常
        :return: bool
        """
        if super(SamplesInfoFile, self).check():
            if not self.check_format:
                raise FileError("文件格式错误")
        return True

    def get_file_info(self):
        """
        获取samples_info文件的信息
        row: 行数
        colname: 列名
        samples: 样本数
        stat: 统计信息
        """
        row = 0
        colname = list()
        stat = {'reads': 0, 'bases': 0, 'min': Decimal('Infinity'), 'max': 0, 'avg': 0}
        with open(self.prop['path'], 'r') as f:
            for line in f:
                line = line.rstrip("\n")
                line = re.split("\t", line)
                row += 1
                if row == 1:
                    for i in range(0, len(line)):
                        colname.append(line[i])
                else:
                    for i in range(0, len(line)):
                        if colname[i] == "reads":
                            stat['reads'] = str(int(stat['reads']) + int(line[i]))
                        if colname[i] == "bases":
                            stat['bases'] = str(int(stat['bases']) + int(line[i]))
                        if colname[i] == "min":
                            stat['min'] = min(line[i], stat['min'])
                        if colname[i] == "max":
                            stat['max'] = max(line[i], stat['max'])
                        if colname[i] == "avg":
                            stat['avg'] = str(int(stat['avg']) + int(line[i]))
        print row
        samples = row - 1
        stat['avg'] = int(stat['avg']) / int(samples)
        return samples, stat['reads'], stat['bases'], stat['avg'], stat['min'], stat['max']

    @property
    def check_format(self):
        """
        检测文件头是否符合要求
        """
        row = 0
        head = ["sample", "reads", "min", "max", "avg", "bases"]
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
