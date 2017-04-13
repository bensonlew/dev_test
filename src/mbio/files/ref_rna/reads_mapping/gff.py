# -*- coding: utf-8 -*-
# __author__ = 'shijin'

from biocluster.iofile import File
from biocluster.core.exceptions import FileError
import subprocess
import os
from biocluster.config import Config

class GffFile(File):
    """
    定义gff格式文件
    """

    def __init__(self):
        super(GffFile, self).__init__()
        self.gffread_path = Config().SOFTWARE_DIR + "/bioinfo/rna/cufflinks-2.2.1/"
        self.bedops_path = Config().SOFTWARE_DIR + "/bioinfo/align/bedops/bin/"
        self.gtf2bed_path = Config().SOFTWARE_DIR + "/bioinfo/rna/scripts/gtf2bed.pl"

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

    def gff_to_gtf(self):
        """
        gff格式转gtf格式
        """
        gtf_path = os.path.split(self.prop['path'])[0]
        gtf = os.path.join(gtf_path, os.path.split(self.prop['path'])[1] + ".gtf")
        cmd = "{}gffread {} -T -O -o {}".format(self.gffread_path, self.prop['path'], gtf)
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            raise Exception("运行出错！")
        return True

    def gtf_to_bed(self):
        """
        gtf格式转bed格式
        """
        bed_path = os.path.split(self.prop['path'])[0] 
        gtf = os.path.join(bed_path, os.path.split(self.prop['path'])[1] + ".gtf")
        bed = os.path.join(bed_path, os.path.split(self.prop['path'])[1] + ".bed") 
        cmd = "perl {} {} > {}".format(self.gtf2bed_path, gtf, bed)
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            os.remove(bed)
            raise Exception("运行出错")
        return True

if __name__ == '__main__':
    a = GffFile()
    # a.set_path("")
    # a.check()
    # a.gff_to_gtf()
    # a.gtf_to_bed()
