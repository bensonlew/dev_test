# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.core.exceptions import FileError
from biocluster.iofile import Directory
from mbio.files.align.blast.blast_xml import BlastXmlFile
import os


class BlastXmlDirFile(Directory):
    """
    bam文件夹格式
    """
    def __init__(self):
        super(BlastXmlDirFile, self).__init__()

    def check(self):
        if super(BlastXmlDirFile, self).check():
            self.get_info()
            return True

    def get_info(self):
        files = os.listdir(self.path)
        self.set_property('files_num', len(files))
        if not len(files):
            raise FileError('文件夹为空，请检查！')
        for f in files:
            blasttable = BlastXmlFile()
            blasttable.set_path(self.path + '/' + f)
            blasttable.check()
