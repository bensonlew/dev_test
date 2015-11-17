# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import os
import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError

class EstimatorsFile(File):
    """
    定义estimators文件格式
    """
    def __init__(self):
        super(EstimatorsFile,self).__init__()
        
    def get_info(self):
        """
        获取文件属性
        """
        super(EstimatorsFile, self).get_info()
        info = self.get_Estimators_info()
        self.set_property('file_name',info[0])
        # self.set_property('estimators_type',info[2])
        self.set_property('sample_num',info[1])
       
    def get_Estimators_info(self):
        """
        获取指数表信息
        """
        file_name,sample_num = '',0
        file_name = os.path.basename(self.prop['path'])
        f = open(self.prop['path'], 'r')
        lines = f.readlines()
        sample_num = len(lines) - 1
        return (file_name,sample_num)

    def check(self):
        if super(EstimatorsFile,self).check():
            pass
        else:
            raise FileError("文件格式错误")
        return True

