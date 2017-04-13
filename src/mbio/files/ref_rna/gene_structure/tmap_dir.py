# -*- coding: utf-8 -*-

# __author__ = 'linfang.jin'
# time: 2017/1/22 14:41

import re
from biocluster.iofile import Directory
from biocluster.core.exceptions import FileError


class TmapDirFile(Directory):
    """
    bam文件夹格式
    """
    def __init__(self):
        super(TmapDirFile, self).__init__()

    def check(self):
        if super(TmapDirFile, self).check():
            return True
