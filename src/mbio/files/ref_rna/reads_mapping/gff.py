# -*- coding: utf-8 -*-
# __author__ = 'sj'

from biocluster.iofile import File
from biocluster.core.exceptions import FileError
import subprocess
import os

class GffFile(File):
    """
    定义gff格式文件
    """

    def __init__(self):
        super(GffFile, self).__init__()
        self.gffread_path = "/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/cufflinks-2.2.1/"
        self.bedops_path = "/mnt/ilustre/users/sanger-dev/app/bioinfo/align/bedops/bin/"

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(GffFile, self).check():
            super(GffFile,self).get_info()
            with open(self.prop["path"],"r") as f:
                for line in f:
                    if line.find("#") != 0:
                        line = line.strip()
                        lst = line.split("\t")
                        # print lst
                        if len(lst) != 9:
                            raise FileError("文件格式错误，gff应有九列")
                        else:
                            return True
                            
        else:
            raise FileError("文件格式错误") 
		
    def gff_to_bed(self):
        bed_path = os.path.split(self.prop['path'])[0]
        bed = os.path.join(bed_path, os.path.split(self.prop['path'])[1] + ".bed")
        cmd = "perl {}gff2bed -d <{} >{}".format(self.bedops_path,self.prop['path'],bed)
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            raise Exception("运行出错！")
        return True
		
    def gff_to_gtf(self):
        gtf_path = os.path.split(self.prop['path'])[0]
        gtf = os.path.join(gtf_path,os.path.split(self.prop['path'])[1] + ".gtf")
        cmd = "{}gffread {} -T -o {}".format(self.gffread_path, self.prop['path'], gtf)
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            raise Exception("运行出错！")
        return True
"""
if __name__ == '__main__':
    a = GffFile()
    a.set_path("/mnt/ilustre/users/sanger-dev/app/database/refGenome/Animal/Birds_Reptil/Chicken/Ensembl84/Gallus_gallus.Galgal4.84.gff3")
    a.check()
    a.gff_to_bed()
"""    