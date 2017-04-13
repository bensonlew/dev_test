# -*- coding: utf-8 -*-
# __author__ = 'shijin'

from biocluster.iofile import File
from biocluster.core.exceptions import FileError
import subprocess
import os
from biocluster.config import Config


class GtfFile(File):
    """
    定义Gtf格式文件
    """

    def __init__(self):
        super(GtfFile, self).__init__()
        self.gtf_to_gff_path =Config().SOFTWARE_DIR + "/bioinfo/align/scripts/"
        self.bedops_path = Config().SOFTWARE_DIR +"/bioinfo/align/bedops/bin/"
        self.gtf2bed_path = Config().SOFTWARE_DIR + "/bioinfo/rna/scripts/gtf2bed.pl"

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(GtfFile, self).check():
            super(GtfFile, self).get_info()
            with open(self.prop["path"], "r") as f:
                for line in f:
                    if line.find("#") == -1:
                        line = line.strip()
                        lst = line.split("\t")
                        if len(lst) != 9:
                            raise FileError("文件格式错误,gtf应有9列")
                        else:
                            return True
        else:
            raise FileError("文件格式错误")

    def gtf_to_gff(self):
        """
        gtf格式转gff格式
        """
        cmd = "perl {}gtf2gff.pl {}".format(self.gtf_to_gff_path, self.prop['path'])
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
        bed = os.path.join(bed_path, os.path.split(self.prop['path'])[1] + ".bed")
        cmd = "perl {} {} > {}".format(self.gtf2bed_path,self.prop['path'],bed)
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            os.remove(bed)
            raise Exception("运行出错")
        return True


if __name__ == '__main__':
    a = GtfFile()
    a.set_path("")