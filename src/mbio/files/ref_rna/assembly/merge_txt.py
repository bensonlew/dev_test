# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'

"""txt格式文件类"""

from biocluster.iofile import File
from biocluster.core.exceptions import OptionError


class MergeTxtFile(File):
    """
    txt类
    """
    def __init__(self):
        super(MergeTxtFile, self).__init__()
        
    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(MergeTxtFile, self).check():
            super(MergeTxtFile,self).get_info()
            with open(self.prop["path"],"r") as f:
                for line in f:
                    if line.find("/") != 0:
                        raise FileError("文件格式错误，文本路径为绝对路径")
                    else:
                        return True
        else:
            raise FileError("文件格式错误")
