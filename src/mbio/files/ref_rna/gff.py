# -*- coding: utf-8 -*-
# __author__ = 'sj'

from biocluster.iofile import File
from biocluster.core.exceptions import FileError
import subprocess


class GffFile(File):
    """
    定义gff格式文件
    """

    def __init__(self):
        super(GffFile, self).__init__()
        self.gffread_path = "rna/cufflinks-2.2.1/"
		self.bedops_path = "align/bedops/bin/"

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(GffFile, self).check():
            return True
        else:
            raise FileError("文件格式错误")

    def gff_to_gtf(self, gtf):
        cmd = "{}gffread  {} -o {}" .format(self.gffread_path, self.prop['path'], gtf)
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            raise Exception("运行出错！")
        return True
		
	def gff_to_bed(self,bed):
		cmd = "perl {}gff2bed -d <{} >{}".format(self.bedops_path,self.prop['path'],bed)
		try:
			subprocess.check_output(cmd, shell=True)
		except subprocess.CalledProcessError:
			raise Exception("运行出错！")
		return True