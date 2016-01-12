# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

import re


def search_fasta_by_id(fasta, fasta_id):
    try:
        with open(fasta, 'r') as f:
            line_list = f.readlines()
            # print line_list
    except IOError:
        print '无法打开fasta文件'
    with open('searchID_result.fasta', 'w') as out_file:
        match = 0
        for i in fasta_id.split(','):
            # print i
            for line in line_list:
                if re.match(r'>', line) and i in line:
                    id_index = line_list.index(line)
                    id_fasta = line_list[id_index + 1]
                    out_file.write('%s%s' % (line_list[id_index], id_fasta))
                    match += 1


def search_fastq_by_id(fastq, fastq_id):
    try:
        with open(fastq, 'r') as f:
            line_list = f.readlines()
            # print line_list
    except IOError:
        print '无法打开fasta文件'
    with open('searchID_result.fastq', 'w') as out_file:
        match = 0
        for i in fastq_id.split(','):
            for line in line_list:
                    if re.match(r'@', line) and i in line:
                        id_index = line_list.index(line)
                        id_fastq = line_list[id_index + 1]
                        next_line = line_list[id_index + 2]
                        quality_line = line_list[id_index + 3]
                        out_file.write('%s%s%s%s' % (line_list[id_index], id_fastq, next_line, quality_line))
                        match += 1


def search_fasta_by_idfile(fasta, id_file):
    fasta_id_list = []
    try:
        with open(fasta, 'r') as f:
            line_list = f.readlines()
            # print line_list
        with open(id_file) as idfile:
            for fasta_id in idfile.readlines():
                if re.match('[\t\n]+', fasta_id):
                    pass
                else:
                    fasta_id_list.append(fasta_id.strip('\r\n'))
            print fasta_id_list
    except IOError:
            print '无法打开文件'
    with open('searchID_result.fasta', 'w') as out_file:
        match = 0
        for fasta_id in fasta_id_list:
            for line in line_list:
                if re.match(r'>', line) and fasta_id in line:
                    id_index = line_list.index(line)
                    id_fasta = line_list[id_index + 1]
                    out_file.write('%s%s' % (line_list[id_index], id_fasta))
                    match += 1


def search_fastq_by_idfile(fastq, id_file):
    fastq_id_list = []
    try:
        with open(fastq, 'r') as f:
            line_list = f.readlines()
            # print line_list
        with open(id_file) as idfile:
            for fastq_id in idfile.readlines():
                if re.match('[\t\n]+', fastq_id):
                    pass
                else:
                    fastq_id_list.append(fastq_id.strip('\r\n'))
            print fastq_id_list
    except IOError:
            print '无法打开文件'
    with open('searchID_result.fastq', 'w') as out_file:
        match = 0
        for fastq_id in fastq_id_list:
            for line in line_list:
                if re.match(r'@', line) and fastq_id in line:
                    id_index = line_list.index(line)
                    id_fastq = line_list[id_index + 1]
                    next_line = line_list[id_index + 2]
                    quality_line = line_list[id_index + 3]
                    out_file.write('%s%s%s%s' % (line_list[id_index], id_fastq, next_line, quality_line))
                    match += 1
