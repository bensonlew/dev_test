# -*- coding: utf-8 -*-

"""express output —— gene count file"""

from biocluster.iofile import File
from biocluster.core.exceptions import OptionError

class CountFile(File):
    
    """gene count table 类"""
    
    def __init__(self):
        super(CountFile,self).__init__()
        
    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        """
        if super(CountFile,self).check():
            return True
        else:
            raise FileError("文件格式错误")
