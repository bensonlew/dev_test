# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""lda_table格式文件类"""

from biocluster.iofile import File
import re
import subprocess
from biocluster.config import Config
import os
from biocluster.core.exceptions import FileError


class LdaTableFile(File):
    """
    定义LdaTable文件
    """
    def __init__(self):
        super(LdaTableFile,self).__init__()

    def check(self):
        """
        检测文件是否满足要求
        :return:
        """
        super(LdaTableFile,self).check()

    def get_info(self):
        """
        获取文件属性
        """
        super(LdaTableFile, self).get_info()
        info = self.get_file_info()
        self.set_property("organism_number",len(info))
        self.set_property("organism_name",info)


    def get_file_info(self):
        """
        获取lda_table文件的信息
        """
        name_list = {}
        f = open(self.prop['path'],'r')
        for line in f:
           foo = line.split('\t',1)
           name_list.append(foo[0])
        f.close()
        return name_list

    def get_use_info(self,use_file_path):
        """
        获取lda判别有效信息行的内容
        """
        f = open(self.prop['path'],'r')
        w = open("%s" % use_file_path,'w')
        for line in f:
            if "-" in line:
                pass
            else:
                w.write(line)



