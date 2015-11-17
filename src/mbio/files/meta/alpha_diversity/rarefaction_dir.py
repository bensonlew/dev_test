# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

import os
import re
from biocluster.iofile import Directory
from biocluster.core.exceptions import FileError


class RarefactionDirFile(Directory):
    """
    定义Rarefaction格式文件夹
    """
    def __init__(self):
        super(RarefactionDirFile,self).__init__()

    def get_info(self):
        """
        获取文件夹属性
        """
        super(RarefactionDirFile, self).get_info()
        info = self.get_rarefaction_dir_info()
        self.set_property('file_name',info[0])
        self.set_property('sample_num',info[1])
        self.set_property('sample_name',info[2])

    def get_rarefaction_dir_info(self):
        """
        获取文件夹信息
        """
        file_name = os.listdir(self.prop('path'))
        sample_num = len(file_name)
        sample_name = []
        maker = '.rarefaction'
        maker_num = 0
        for f_n in file_name:
            s = re.search('[.](\S+)[.]',f_n)
            sample_name.append(s.group(1))
            if maker in f_n:
                maker_num += 1
        if maker_num == sample_num:
            check = 'True'
        else:
            check = 'False'
        return(file_name,sample_num,sample_name,check)


    def check(self):
        """
        检测文件夹是否满足要求，不满足时报错
        """
        check = self.get_rarefaction_dir_info()
        if super(RarefactionDirFile,self).check():
            if check[3] == 'True':
                pass
            else:
                raise FileError("文件格式错误")
        return True


