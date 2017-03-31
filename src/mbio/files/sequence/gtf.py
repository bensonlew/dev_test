# -*- coding: utf-8 -*-
# __author__ = fiona
# time: 2017/3/10 18:08


import re, os, Bio, argparse, sys, fileinput, urllib2
import Bio
import subprocess
from bs4 import BeautifulSoup
from iofile import File
from gff3 import Gff3File
import regex
import logging
from collections import defaultdict

'''
gtf:gene transefer format
格式说明地址：http://mblab.wustl.edu/GTF22.html


'''


def check_seq_type(seq_type):
    return True


def dict_factory():
    return defaultdict(dict_factory)


class GtfFile(File):
    def __init__(self, fasta):  # fasta应为FastaFile对象
        self._validate_gtf_tool = 'validate_gtf.pl'  # 此脚本
        self._co_fasta = fasta
        self._contig_info = {}
        '''
        {contig:{
            gene1:{
                txpt1:{
                    'TSS':{start:**,end:**},
                    'exon1':{start:**,end:**},
                    'cds': {start:**,end:**},
                    '3_UTR':{}
                    },
                txpt2:{
                
                }
        '''
        self._structure_hierachy = dict_factory()
    
    def check(self, detail):
        super(GtfFile, self).check()
        pass
    
    def __check_gtf_bio_logic(self):
        '''
        此方法使用validate_gtf.pl文件检查gtf以下内容
        :return:
        '''
        if self._validate_gtf_tool:
            tmp_out_txpt = os.path.join(os.path.dirname(self.path), os.path.basename(self.path) + '_tmp_txpt.gtf')
            validate_gtf_cmd = 'perl {} -fsm -t {} {} {}'.format(self._validate_gtf_tool, tmp_out_txpt,
                                                                 self._co_fasta.path, )
    
    def __check_skechy(self):
        '''
        
        粗略检查: 检查各个字段的字符是否符合规范
        1. tab分隔为9列
        2. 第九列 两个必须有的：gene_id value  ranscript_id value
        :return:
        '''
        for line in open(self.path):
            comment_m = re.match(r'^#.+', line.strip())
            content_m = regex.match(
                r'^([^#]\S*?)\t+((\S+)\t+){7,7}((transcript_id|gene_id)\s+?\"(\S+?)\");.*((transcript_id|gene_id)\s+?\"(\S+?)\");(.*;)*$',
                line.strip())
            
            if not (comment_m or content_m):
                raise Exception(
                    'line {} is illegal in gtf file {}: it is not comment line(start with #) or tab-delimeted 9 colomuns line(the No9 line must contain gene_id txptid ) ')
    
    def _check_chars(self):
        '''
        基本检查: 检查各个字段的字符是否符合规范
        1. tab分隔为9列
        2. 第九列 两个必须有的：gene_id value  ranscript_id value
        3. 每一列符合他们应有的规范
        :return:
        '''
        
        
        for line in open(self.path):
            
            comment_m = re.match(r'^#.+', line.strip())
            content_m = regex.match(
                r'^([^#]\S*?)\t+((\S+)\t+){7,7}((transcript_id|gene_id)\s+?\"(\S+?)\");.*((transcript_id|gene_id)\s+?\"(\S+?)\");(.*;)*$',
                line.strip())
            
            if not (comment_m or content_m):
                raise Exception(
                    'line {} is illegal in gtf file {}: it is not comment line(start with #) or tab-delimeted 9 colomuns line(the No9 line must contain gene_id txptid ) ')
            
            if content_m:
                contig = content_m.captures(1)[0]
                seq_type = content_m.captures(2)[1].strip()
                start = content_m.captures(2)[2].strip()
                end = content_m.captures(2)[3].strip()
                frame = content_m.captures(2)[5].strip()
                strand = content_m.captures(2)[6].strip()
                contig_m = re.match(r'^[\w.:^*$@!+?-|]+$', contig)  # contig的字符必须在[\w.:^*$@!+?-|]之内
                seq_type_m = check_seq_type(seq_type)  # seq_type必须在SO term集合之内
                start_m = re.match(r'^\d+$', start)
                end_m = re.match(r'^\d+$', end)
                frame_m = re.match(r'^[\.120]$', frame)
                strand_m = re.match(r'^[\.\?\-\+]$', strand)
                if not (contig_m and seq_type_m and start_m and frame_m and end_m and strand_m):
                    raise Exception('line {} in gtf file {} is not legal.'.format(line.strip(), self.path))
                
    
    def __check_hierachy(self):
        '''
        包含__check_chars的功能
        :return:
        '''
        for line in open(self.path):
        
            comment_m = re.match(r'^#.+', line.strip())
            content_m = regex.match(
                r'^([^#]\S*?)\t+((\S+)\t+){7,7}((transcript_id|gene_id)\s+?\"(\S+?)\");.*((transcript_id|gene_id)\s+?\"(\S+?)\");(.*;)*$',
                line.strip())
        
            if not (comment_m or content_m):
                raise Exception(
                    'line {} is illegal in gtf file {}: it is not comment line(start with #) or tab-delimeted 9 colomuns line(the No9 line must contain gene_id txptid ) ')
        
            if content_m:
                contig = content_m.captures(3)[0]
                seq_type = content_m.captures(3)[1]
                start = content_m.captures(3)[2]
                end = content_m.captures(3)[3]
                frame = content_m.captures(3)[5]
                strand = content_m.captures(3)[6]
                contig_m = re.match(r'^[\w.:^*$@!+?-|]+$', contig)  # contig的字符必须在[\w.:^*$@!+?-|]之内
                seq_type_m = check_seq_type(seq_type)  #
                start_m = re.match(r'^\d+$', start)
                end_m = re.match(r'^\d+$', end)
                frame_m = re.match(r'^[\.120]$', frame)
                strand_m = re.match(r'^[\.\?\-\+]$', strand)
                
                if gene_id:
                
 
                gene_id = content_m.captures()
                if not (contig_m and seq_type_m and start_m and frame_m and end_m and strand_m):
                    raise Exception('line {} in gtf file {} is not legal.'.format(line.strip(), self.path))
                self._structure_hierachy[contig][]
    
    def check_gtf_for_merge(self):
        '''
        检查merged.gtf每一行的第九列
        :return:
        '''
    
    def parse_details(self):
        pass
    
    def parse(self):
        pass
    
    def to_bed(self):
        pass
    
    def gtf_tbi(self):
        pass
