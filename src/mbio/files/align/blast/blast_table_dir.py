# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.iofile import Directory
import os
from biocluster.core.exceptions import FileError
from mbio.files.align.blast.blast_table import BlastTableFile


class BlastTableDirFile(Directory):
    """
    bam文件夹格式
    """
    def __init__(self):
        super(BlastTableDirFile, self).__init__()

    def check(self):
        if super(BlastTableDirFile, self).check():
            self.get_info()
            return True

    def get_info(self):
        files = os.listdir(self.path)
        self.set_property('files_num', len(files))
        if not len(files):
            raise FileError('文件夹为空，请检查！')
        for f in files:
            blasttable = BlastTableFile()
            blasttable.set_path(self.path + '/' + f)
            blasttable.check()
