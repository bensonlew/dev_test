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

'''
gtf:gene transefer format
格式说明地址：http://mblab.wustl.edu/GTF22.html


'''


class GtfFile(File):
    def __init__(self, fasta):  # fasta应为FastaFile对象
        self._validate_gtf_tool = 'validate_gtf.pl'  # 此脚本
        self._co_fasta = fasta
    
    def check(self, detail):
        
        pass
    
    def _check_gtf_bio_content(self):
        if self._validate_gtf_tool:
            tmp_out_txpt = os.path.join(os.path.dirname(self.path), os.path.basename(self.path) + '_tmp_txpt.gtf')
            validate_gtf_cmd = 'perl {} -fsm -t {} {} {}'.format(self._validate_gtf_tool, tmp_out_txpt,
                                                                 self._co_fasta.path, )
    
    def check_basic(self):
        '''
        基本检查：
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
            
            if content_m:
                pass
        
        pass
    
    def _check_details(self):
        pass
    
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
