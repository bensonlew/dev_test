# -*- coding: utf-8 -*-

# __author__ = 'linfang.jin'
# time: 2017/1/22 14:40

import re
from biocluster.iofile import Directory
from biocluster.core.exceptions import FileError


class GtfDirFile(Directory):
    """
    bam文件夹格式
    """
    def __init__(self):
        super(GtfDirFile, self).__init__()

    def check(self):
        if super(GtfDirFile, self).check():
            return True
