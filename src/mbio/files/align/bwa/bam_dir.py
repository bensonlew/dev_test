# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import re
from biocluster.iofile import Directory
from biocluster.core.exceptions import FileError


class BamDirFile(Directory):
    """
    bam文件夹格式
    """
    def __init__(self):
        super(BamDirFile, self).__init__()

    def check(self):
        if super(BamDirFile, self).check():
            return True
