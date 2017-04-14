#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import os
import subprocess
from biocluster.iofile import Directory
from biocluster.core.exceptions import FileError
from mbio.files.sequence.fastq import FastqFile
from mbio.files.sequence.file_sample import FileSampleFile

class KallistoDir(Directory):
    """
    定义kallisto结果目录文件夹
    """
    def __init__(self):
        super(KallistoDir, self).__init__()
    
    def check(self):
        if super(KallistoDir, self).check():
            self.get_info()
            pass
        else:
            return FileError("kallistoDir文件格式错误")
    
    def get_info(self):
        if os.path.exists(self.prop['path']):
            for files in os.listdir(self.prop['path']):
                _file_path = os.path.join(self.prop['path'], files)
                check_result = self.get_single_sample_info(_file_path)
        
    def remove_redundancy(self, file_path):
        os.system("sed -i 's/transcript://g' {}".format(file_path))
        print "除去冗余转录本名称"
        
    def get_single_sample_info(self, file_path):
        """读取每个tsv文件"""
        with open(file_path, 'r+') as f:
            try:
                f.readline()
            except Exception:
                print "{}文件为空！".format(file_path)
            self._head = f.readline().strip().split("\t")
            if len(self._head) == 5:
                if self._head[0].startswith("transcript"):
                    self.remove_redundancy(file_path = file_path)
                return True
            else:
                raise Exception("{}文件应该是5列")
                
if __name__ == "__main__":
    data=KallistoDir()
    data.set_path("/mnt/ilustre/users/sanger-dev/workspace/20170125/Single_express_kallisto_v1/Express/output/kallisto")
    data.get_info()