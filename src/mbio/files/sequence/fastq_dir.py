# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
import os
import subprocess
from biocluster.config import Config
from biocluster.core.exceptions import FileError
from biocluster.iofile import Directory


class FastqFileDir(Directory):
    """
    定义fastq文件夹
    需要安装gzip
    需要安装fastoolkit，命令fastq_to_fasta用于将fastq转化为fasta
    """
    def __init__(self):
        """
        :param fastqs: 不带路径的fastq的文件名集合
        """
        super(FastqFileDir, self).__init__()
        self.fastq_to_fasta_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/fastq_to_fasta")
        self.fastqs = list()
        self.is_converted = False

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' in self.prop.keys() and os.path.isdir(self.prop['path']):
            self.set_property("fastq_number", self.get_fastq_number())
        else:
            raise FileError(u"文件夹路径不正确，请设置正确的文件夹路径!")

    def get_fastq_number(self):
        """
        获取文件夹下fastq的数目
        :return:文件数目
        """
        filelist = os.listdir(self.prop['path'])
        count = 0
        for file_ in filelist:
            if re.search(r'\.(fastq|fq)$', file_) or re.search(r'\.(fastq|fq)\.gz$', file_):
                count += 1
                self.fastqs.append(file_)
        return count

    def covert_to_fasta(self):
        """
        将所有的fastq转化为fasta文件，当fastq是gz格式的时候，先解压再转化
        :return:转化后的fasta文件夹地址
        """
        tmp_dir = self._make_tmp_dir(self)
        if not self.is_convert:
            os.mkdir(tmp_dir + '/converted_fastas')
            for fastq in self.fastqs:
                fastq = self.prop['path'] + "/" + fastq
                # 转化gz文件到fastq
                if re.search(r'\.(fastq|fq)\.gz', fastq):
                    ungz_name = re.search(r'(.+)\.(fastq|fq)\.gz$').group(1)
                    new_fastq = tmp_dir + "/" + ungz_name + ".fastq"
                    fasta = tmp_dir + '/converted_fastas/' + ungz_name + ".fasta"
                    try:
                        subprocess.check_call('gunzip -c ' + fastq + " > " + new_fastq, shell=True)
                    except subprocess.CalledProcessError:
                        raise Exception(u"解压缩文件失败!")
                    fastq = new_fastq
                else:
                    fasta = re.search(r'(.+)\.(fastq|fq)').group(1)
                    fasta = tmp_dir + '/converted_fastas/' + fasta + ".fasta"
                # 转化fastq 到fasta
                try:
                    convert_str = (self.fastq_to_fasta_path + ' -Q 33' + ' -i '
                                   + fastq + ' -o ' + fasta)
                    subprocess.check_call(convert_str, shell=True)
                    self.is_convert = True
                except subprocess.CalledProcessError:
                    raise Exception(u'fastq转化fasta失败！')
        return tmp_dir + '/converted_fastas'

    def check(self):
        """
        检测文件夹是否满足要求，不满足时触发FileError异常
        :return:
        """
        if super(FastqFileDir, self).check():
            if "file_number" not in self.prop.keys():
                raise FileError(u"还未设置该文件夹下的fastq文件数目")
            if self.prop['file_number'] != self.get_fastq_number():
                raise FileError(u"实际fastq文件数目不等于设定值")
