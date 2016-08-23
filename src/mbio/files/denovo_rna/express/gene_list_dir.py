# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import re
from biocluster.iofile import Directory
from biocluster.core.exceptions import FileError


class GeneListDirFile(Directory):
    """
    差异基因文件夹格式
    """
    def __init__(self):
        super(GeneListDirFile, self).__init__()

    def check(self):
        if super(GeneListDirFile, self).check():
            return True
