# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import re
import os
import subprocess
from Bio import SeqIO
from biocluster.core.exceptions import FileError
from biocluster.iofile import Directory


class FastaFileDir(Directory):
    """
    定义fasta文件夹
    需要biopython
    """
    def __init__(self):
        """
        :param fastqs: 不带路径的fastq的文件名集合
        """
        super(FastaFileDir, self).__init__()
        self.fastas = list()

    def get_info(self):
        """
        获取文件夹属性
        """
        if 'path' in self.prop.keys() and os.path.isdir(self.prop['path']):
            self.set_property("fasta_number", self.get_fasta_number())
        else:
            raise FileError(u"文件夹路径不正确，请设置正确的文件夹路径!")

    def get_fasta_number(self):
        """
        获取文件夹下fasta的数目
        :return:文件数目
        """
        filelist = os.listdir(self.prop['path'])
        count = 0
        for file_ in filelist:
            if re.search(r'\.(fasta|fa)$', file_):
                count += 1
                self.fastas.append(file_)
        return count

    def cat_fastas(self):
        """
        将所有的fasta文件合并到一起
        :return: 合并到一起的fasta文件路径
        """
        tmp_dir = self._make_tmp_dir()
        cat_fasta = tmp_dir + "/cat_fasta.fasta"
        if os.path.exists(cat_fasta):
            os.remove(cat_fasta)
        os.mknod(cat_fasta)
        for fasta in self.fastas:
            try:
                cat_str = "cat " + fasta + " >> " + cat_fasta
                subprocess.check_call(cat_str, shell=True)
                return cat_fasta
            except subprocess.CalledProcessError:
                error = u"合并 " + fasta + u" 文件时出错"
                raise Exception(error)

    def cat_fastas_for_meta(self):
        """
        将所有的fasta按照一定的规则合并到一起
        规则: 检测fasta序列名里面有没有下划线_,有的话替换成单横杠-
              在序列名前加上文件名，与原序列名用下划线_隔开
              将所有的fasta合并到一起
        :return: 合并到一起的fasta文件的路径
        """
        tmp_dir = self._make_tmp_dir()
        cat_fasta = tmp_dir + "/cat_meta.fasta"
        if os.path.exists(cat_fasta):
            os.remove(cat_fasta)
        os.mknod(cat_fasta)
        for fasta in self.fastas:
            basename = os.path.basename(fasta)
            sample_name = re.search(r'(.+)\.(fasta|fa)$', basename).group(1)
            with open(cat_fasta, "a") as f:
                for seq in SeqIO.parse(fasta, "fasta"):
                    new_id = str(sample_name) + '_' + str(seq.id)
                    f.write(new_id + "\n")
                    f.write(seq.seq + "\n")
        return cat_fasta

    def check(self):
        """
        检测文件夹是否满足要求，不满足时触发FileError异常
        :return:
        """
        if super(FastaFileDir, self).check():
            if "file_number" not in self.prop.keys():
                raise FileError(u"还未设置该文件夹下的fastq文件数目")
            if self.prop['file_number'] != self.get_fasta_number():
                raise FileError(u"实际fasta文件数目不等于期望值")
