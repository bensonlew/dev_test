# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import re
import subprocess
from biocluster.iofile import File
from biocluster.config import Config
from biocluster.core.exceptions import FileError


class FastqFile(File):
    """
    定义Fastq文件，
    需要安装gzip
    需要安装fastoolkit，命令fastq_to_fasta用于将fastq转化为fasta
    需要安装seqstat软件，版本1.9g(Oct 2002),软件用法:  seqstat <fasta文件>，根据stdout的输出，统计fasta的信息
    :param _fastaname:由fastq文件转化而来的fasta文件
    :param _filename: 若fastq为gz格式,为解压后fastq文件，若fastq不是gz格式，等于prop["path"]
    :param is_convert: 是否已经转化成fasta
    """
    def __init__(self):
        super(FastqFile, self).__init__()
        self.seqstat_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/seqstat")
        self.fastq_to_fasta_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/fastq_to_fasta")
        self._fastaname = ""
        self._filename = ""
        self.is_convert = False

    @property
    def is_gz(self):
        """
        依据文件后缀名检测是是gz个是压缩文件
        """
        filename = self.prop['path']
        if re.search(r'\.gz$', filename):
            return True
        else:
            return False

    def get_info(self):
        """
        获取文件属性
        当fastq是gz格式的时候，新建一个tmp文件夹，把文件解压到tmp里
        将fastq转化成fasta，并统计相关信息
        :return:
        """
        super(FastqFile, self).get_info()
        self._prepare()
        self.convert_to_fasta()
        seqinfo = self.get_seq_info()
        self.set_property("format", "FASTQ")
        self.set_property("fasta_fomat", seqinfo[0])
        self.set_property("seq_number", seqinfo[1])
        self.set_property("bases", seqinfo[2])
        self.set_property("longest", seqinfo[3])
        self.set_property("shortest", seqinfo[4])

    def check(self):
        """
        检测文件是否满足要求,发生错误时应该触发FileError异常
        :return: bool
        """
        if super(FastqFile, self).check():
            if self.prop['fasta_format'] != 'DNA':
                raise FileError("文件格式错误")
            if self.prop["seq_number"] < 1:
                raise FileError("应该至少含有一条序列")
        return True

    def _prepare(self):
        """
        为获取序列的信息做准备
        生成临时文件夹，当输入的文件是gz格式时，解压到tmp里
        """
        filename = self.prop['path']
        basename = os.path.basename(filename)
        filepath = os.path.dirname(os.path.abspath(filename))
        os.mkdir(filepath + "/tmp")
        fastaname = filepath + "/tmp/" + basename + ".fasta"
        if self.is_gz:
            basename = re.search(r'(.+)\.gz', basename).group(1)
            filename = filepath + "/tmp/" + basename
            fastaname = filename + ".fasta"
            try:
                subprocess.check_call('gunzip -c ' + filepath + "> " + filename)
            except subprocess.CalledProcessError:
                raise Exception("非标准格式的gz文件！")
        self.filename = filename
        self.fastaname = fastaname

    def gunzip(self):
        """
        解压fastq,因为在get_info()中，如果判定是压缩文件的话会先解压，所以质粒只需返回文件地址即可
        :return: 解压后的fastq文件
        """
        return self.filename

    def gzip(self):
        """
        当文件不是gz格式的时候，压缩这个fastq，是gz格式的时候，直接返回该文件
        """
        if self.is_gz:
            return self.prop["path"]
        else:
            try:
                gzip_file = self.filename + ".gz"
                gzip_str = "gzip -c " + self.filename + " > " + gzip_file
                subprocess.check_call(gzip_str, shell=True)
                return gzip_file
            except subprocess.CalledProcessError:
                raise Exception("压缩fastq文件失败！")

    def convert_to_fasta(self):
        """
        将fastq转化成fasta
        :return: 转化后的fasta的文件
        """
        if not self.is_convert:
            try:
                convert_str = (self.fastq_to_fasta_path + ' -Q 33' + ' -i '
                               + self.filename + ' -o ' + self.fastaname)
                subprocess.check_call(convert_str, shell=True)
                self.is_convert = True
            except subprocess.CalledProcessError:
                raise Exception('fastq转化fasta失败！')
        return self.fastaname

    def get_seq_info(self):
        """
        获取Fasta信息
        :return: (format,seq_number,bases,longest,shortest)
        """
        try:
            subpro = subprocess.check_output(self.seqstat_path + " " + self.fastaname, shell=True)
            result = subpro.split('\n')
            seq_type = re.split(r':\s+', result[6])[1]
            seq_number = re.split(r':\s+', result[7])[1]
            bases = re.split(r':\s+', result[8])[1]
            shortest = re.split(r':\s+', result[9])[1]
            longest = re.split(r':\s+', result[10])[1]
            return seq_type, seq_number, bases, shortest, longest
        except subprocess.CalledProcessError:
            raise Exception("seqstat 运行出错！")
