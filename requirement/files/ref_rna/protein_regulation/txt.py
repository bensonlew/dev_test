# -*- coding: utf-8 -*-
# __author__ = 'zj'

"""txt格式文件类"""

from biocluster.iofile import File
from biocluster.core.exceptions import OptionError


class TxtFile(File):
    """
    txt类
    """
    def __init__(self):
        super(TxtFile, self).__init__()
        
    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(TxtFile, self).check():
            return True
        else:
            raise FileError("文件格式错误")
