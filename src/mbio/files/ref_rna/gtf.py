# -*- coding: utf-8 -*-
# __author__ = 'sj'

from biocluster.iofile import File
from biocluster.core.exceptions import FileError
import subprocess


class GtfFile(File):
    """
    定义Gtf格式文件
    """

    def __init__(self):
        super(GtfFile, self).__init__()
        self.gtf_to_gff_path = "/align/scripts/"
		self.bedops_path = "align/bedops/bin/"

    def check(self):
        """
        检测文件是否满足要求，发生错误时应该触发FileError异常
        :return:
        """
        if super(GtfFile, self).check():
            return True
        else:
            raise FileError("文件格式错误")

    def gtf_to_gff(self):
        cmd = "perl {}gtf2gff.pl " .format(self.gtf_to_gff_path, self.prop['path'])
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            raise Exception("运行出错！")
        return True
	
	def gtf_to_bed(self,bed):
		cmd = "perl {}gtf2bed -d <{} >{}".format(self.bedops_path,self.prop['path'],bed)
		try:
			subprocess.check_output(cmd, shell=True)
		except subprocess.CalledProcessError:
			raise Exception("运行出错！")
		return True
