# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.iofile import File
import re
import subprocess
from biocluster.config import Config
import os
from biocluster.core.exceptions import FileError


class FastaFile(File):
    """
    定义Fasta文件， 需安装seqstat工具软件
    """
    def __init__(self):
        super(FastaFile, self).__init__()
        self.seqstat_path = os.path.join(Config().SOFTWARE_DIR, "biosquid/bin/seqstat")
        self.sample_name

    def get_info(self):
        """
        获取文件属性
        :return:
        """
        super(FastaFile, self).get_info()
        seqinfo = self.get_seq_info()
        self.set_property("format", seqinfo[0])
        self.set_property("seq_type", seqinfo[1])
        self.set_property("seq_number", seqinfo[2])
        self.set_property("bases", seqinfo[3])
        self.set_property("longest", seqinfo[4])
        self.set_property("shortest", seqinfo[5])

    def check(self):
        """
        检测文件是否满足要求,发生错误时应该触发FileError异常
        :return:
        """
        # print self.prop
        if super(FastaFile, self).check():
            if self.prop['format'] != 'FASTA':
                raise FileError("文件格式错误")
            if self.prop["seq_number"] < 1:
                raise FileError("应该至少含有一条序列")
        return True

    @property
    def sample_name(self):
        """
        :param sample_name: 样本名，为文件名去掉后缀以后的值
        """
        if self.is_set:
            basename = self.prop['basename']
            if re.search(r'\.(fasta|fa)$', basename):
                return re.search(r'(.+)\.(fasta|fa)$', basename).group(1)
            else:
                return basename

    def ncbi_blast_tool_check(self):
        """
        供ncbi.blast Tool检查

        Author: guoquan

        modify: 2015.9.18

        :return:
        """
        if self.check():
            if self.prop['seq_type'] not in {"DNA", "Protein"}:
                raise FileError("不支持此类型的Fasta进行blast比对")
        return True

    def get_seq_info(self):
        """
        获取Fasta信息
        :return: (format,seq_type,seq_number,bases,longest,shortest)
        """
        try:
            subpro = subprocess.check_output(self.seqstat_path + " " + self.prop['path'], shell=True)
            result = subpro.split('\n')
            fformat = re.split(r':\s+', result[5])[1]
            seq_type = re.split(r':\s+', result[6])[1]
            seq_number = re.split(r':\s+', result[7])[1]
            bases = re.split(r':\s+', result[8])[1]
            shortest = re.split(r':\s+', result[9])[1]
            longest = re.split(r':\s+', result[10])[1]
            # print (fformat, seq_type, seq_number, bases, longest, shortest)
            return fformat, seq_type, seq_number, bases, longest, shortest
        except subprocess.CalledProcessError:
            raise Exception("seqstat 运行出错！")

    # def filter(self, smin=0, smax=0):
    #     """
    #     根据长度范围筛选Fasta
    #     :param start:
    #     :param end:
    #     :return:
    #     """
        #
        # def write_seq(wf, sid, seq, smin, smax):
        #     if smin > 0 and len(seq) < smin:
        #         pass
        #     elif smax > 0 and len(seq) > smax:
        #         pass
        #     elif sid == '':
        #         pass
        #     else:
        #         wf.write(">"+sid+"\n"+seq)
        #
        # sid, seq = '', ''
        # wf = open(self.prop['path']+".filter"+smin+"-"+smax+".fa", 'w')
        # with open(self.prop['path'], 'r') as f:
        #     while 1:
        #         line = f.readline().rstrip()
        #         if not line:
        #             write_seq(wf, sid, seq, smin, smax)
        #             break
        #         re_id = re.compile(r'^>(\S+)')
        #         m_id = re_id.match(line)
        #         if m_id is not None:
        #             write_seq(wf, sid, seq, smin, smax)
        #             sid = line
        #             seq = ''
        #         else:
        #             seq += line
        # wf.close()

    def split(self, output, chunk=10000):
        """
        拆分Fasta文件成最大chunk大小的快
        :param output:  String 输出目录
        :param chunk:  int 块大小
        :return:
        """
        s, n = 1, 0
        wf = open("%s/%s.fa" % (output, s), 'w')
        with open(self.prop['path'], 'r') as f:
            while 1:
                line = f.readline()
                if not line:
                    wf.close()
                    break
                re_id = re.compile(r'^>(\S+)')
                m_id = re_id.match(line)
                if m_id is not None:
                    n += 1
                    if n == chunk + 1:
                        wf.close()
                        s += 1
                        n = 0
                        wf = open("%s/%s\.fa" % (output, s), 'w')
                wf.write(line)

    # def get_seq_info(self):
    #     """
    #     获取Fasta信息
    #     :return: (format,type,seq_type,seq_number,bases,longest,shortest)
    #     """
    #     seq_type, seq_number, bases, longest, shortest = '', 0, 0, 0, 0
    #     re_id = re.compile(r'^>(\S+)')
    #     re_na = re.compile(r'^[atcgunATCGUN\*]+$')
    #     re_aa = re.compile(r'^[a-zA-Z\*]+$')
    #     error, seq = 0, ''

    #     def cmp_length(seq, shortest, longest):
    #         """
    #         比较序列长度
    #         """
    #         if len(seq) > 0:
    #             if len(seq) > longest:
    #                 longest = len(seq)
    #             if shortest == 0:
    #                 shortest = len(seq)
    #             elif len(seq) < shortest:
    #                 shortest = len(seq)
    #             else:
    #                 pass
    #         return (seq, shortest, longest)
    #     with open(self.prop['path'], 'r') as f:
    #         while 1:
    #             line = f.readline().rstrip()
    #             if not line:
    #                 shortest, longest = cmp_length(seq, shortest, longest)
    #                 break
    #             m_id = re_id.match(line)
    #             if m_id is not None:
    #                 seq_number += 1
    #                 shortest, longest = cmp_length(seq, shortest, longest)
    #                 seq = ''
    #             else:
    #                 m_na = re_na.match(line)
    #                 if m_na is not None:
    #                     seq_type = "na"
    #                 else:
    #                     m_aa = re_aa.match(line)
    #                     if m_aa is not None:
    #                         seq_type = "aa"
    #                     else:
    #                         error = 1
    #                         break
    #                 bases += len(line)
    #                 seq += line
    #     if error:
    #         return {'pass': False, "info": "文件内容不符合格式!"}
    #     else:
    #         return (seq_type, seq_number, bases, longest, shortest)
