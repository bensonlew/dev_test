# -*- coding: utf-8 -*-

# __author__ = 'linfang.jin'
# time: 2017/1/16 13:33
import re
from biocluster.iofile import File
from biocluster.core.exceptions import FileError

class TmapFile(File):
    """
    tmap文件夹格式
    """
    def __init__(self):
        super(TmapFile, self).__init__()

    def check(self):
        if super(TmapFile, self).check():
            return True