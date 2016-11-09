# -*- coding: utf-8 -*-

"""tablemaker output: ctab format"""

from biocluster.iofile import File
from biocluster.core.exceptions import OptionError

class CtabFile(File):
    """summary 类"""
    def __init__(self):
        super(CtabFile,self).__init__()
        
    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        """
        if super(CtabFile,self).check():
            return True
        else:
            raise FileError("文件格式错误")
