# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class GeneListFile(File):
    """
    gene名字列表文件
    """
    def __init__(self):
        super(GeneListFile, self).__init__()

    def check(self):
        if super(GeneListFile, self).check():
            return True

    def get_one_col(self, output):
        with open(self.prop['path'], 'rb') as r, open(output, 'wb') as w:
            i = 0
            for f in r:
                i += 1
                if i == 1:
                    pass
                else:
                    w.write(f.split('\t')[0] + '\n')
