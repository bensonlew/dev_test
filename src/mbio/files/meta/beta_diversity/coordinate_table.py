# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.iofile import File
# import re
# import subprocess
# from biocluster.config import Config
from biocluster.core.exceptions import FileError


class CoordinateTableFile(File):
    """
    定义多维绘图坐标表文件
    """
    def __init__(self):
        super(CoordinateTableFile, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(CoordinateTableFile, self).get_info()
        tableinfo = self.get_table_info()
        self.set_property('coordinate', tableinfo[0])
        self.set_property('sample', tableinfo[1])

    def get_table_info(self):
        """
        获取并返回坐标表信息

        :return coor, sample:  两个列表，维度名称和样品名称
        """
        tempfile = open(self.prop['path'])
        lines = tempfile.readlines()
        coor = lines[0].rstrip().split('\t')
        sample = [i.split('\t')[0] for i in lines[1:]]
        tempfile.close()
        return coor[1:], sample

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        """
        if super(CoordinateTableFile, self).check():
            # 父类check方法检查文件路径是否设置，文件是否存在，文件是否为空
            tempfile = open(self.prop['path'])
            num = 0
            for i in tempfile:
                if not num:
                    num = len(i.split('\t'))
                    continue
                else:
                    if num == len(i.split('\t')):
                        pass
                    else:
                        raise FileError('存在数据缺失或者数据冗余')
            return True
