# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""stat_table格式文件类"""

from biocluster.iofile import File
import re
import subprocess
from biocluster.config import Config
import os
from biocluster.core.exceptions import FileError


class StatTableFile(File):
    """
    定义Stat_table文件
    """
    def __init__(self):
        super(StatTableFile,self).__init__()

    def check(self):
        """
        检测文件是否满足要求
        :return:
        """
        super(StatTableFile,self).check()

    def get_info(self):
        """
        获取文件属性
        """
        super(StatTableFile, self).get_info()
        number = self.get_organism_number()
        self.set_property("organism_number",number)

    def get_organism_number(self):
        """
        获取文件包含的物种的数量
        """
        f = open(self.prop['path'],'r')
        count = 0
        for i in f:
            count += 1
        num = count - 1
        f.close()   
        return rum

    def get_name(self):
        """
        获取文件中包含的物种的名字
        """
        name_list = []
        f = open(self.prop['path'],'r')
        for line in f:
            if re.match(r'^\t',line):
                pass
            else:
                foo = line.split('\t',1)
                name_list.append(foo[0])
        f.close() 
        return name_list

    def get_test_result(self,name):
        """
        传入某一物种名字时，可以获取该物种名字在各分组（样品）的mean、sd、pvalue、qvalue
        """
        test_result = ''
        head = linecache.getlines(self.prop['path'])[0:2]
        for i in head:        
            test_result += i
        f = open(self.prop['path'],'r')
        for line in f:
            if re.match(r'%s' % name,line):
                test_result += line 
            else:
                pass
        f.close() 
        return test_result
     
