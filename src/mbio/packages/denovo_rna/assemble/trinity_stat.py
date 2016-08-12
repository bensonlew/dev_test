#!/mnt/ilustre/users/sanger/app/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "qiuping"
# last_modify_date:2016.04.08


from Bio import SeqIO
from collections import Counter
import numpy as np


def trinity_stat(fasta, length):
    """
    生成trinity.fasta.stat.xls，length.distribut.txt, unigenes.length.txt, transcript.length.txt
    """
    transcript_detail, transcript_len, gene_len, gene_detail = trinity_info(fasta)
    tran_seq_num, tran_base_num, tran_GC_per, tran_max_len, tran_min_len, tran_average,\
    tran_N50, tran_N90 = stat(transcript_detail, transcript_len)
    gene_seq_num, gene_base_num, gene_GC_per, gene_max_len, gene_min_len, gene_average,\
    gene_N50, gene_N90 = stat(gene_detail, gene_len)
    with open('trinity.fasta.stat.xls', 'wb') as t, open("unigenes.length.txt", "wb") as f1, open("transcripts.length.txt", "wb") as f2:
        t.write('\tunigenes\ttranscripts\ntotal seq num\t%s\t%s\ntotal base num\t%s\t%s\npercent GC\t%s\t%s\n'
                'largest transcript\t%s\t%s\nsmallest transcript\t%s\t%s\naverage length\t%s\t%s\nN50\t%s\t%s\n'
                'N90\t%s\t%s' % (gene_seq_num, tran_seq_num, gene_base_num, tran_base_num, gene_GC_per, tran_GC_per,
                                 gene_max_len, tran_max_len, gene_min_len, tran_min_len, gene_average, tran_average,
                                 gene_N50, tran_N50, gene_N90, tran_N90))
        lengths = length.split(',')
        for one in lengths:
            one = int(one)
            with open("{}_length.distribut.txt".format(one), "wb") as l:
                l.write('length\tunigene_num\tunigene_per\ttranscript_num\ttranscript_per\n')
                transcript_len_range, tran_sum, tran_len_sum = len_stat(transcript_len, one)
                gene_len_range, gene_sum, gene_len_sum = len_stat(gene_len, one)
                gene_len_range.keys().sort()
                for key in gene_len_range.keys():
                    l.write('%s\t%s\t%s\t%s\t%s\n' % (key, gene_len_range[key], '%0.4g' %
                                                      (gene_len_range[key] * 100.0 / gene_sum), transcript_len_range[key],
                                                      '%0.4g' % (transcript_len_range[key] * 100.0 / tran_sum)))
        f1.write('length\tunigenes_number\n')
        for i in gene_len_sum:
            f1.write('%s\t%s\n' % (i[0], i[1]))
        f2.write('length\ttranscript_number\n')
        for i in tran_len_sum:
            f2.write('%s\t%s\n' % (i[0], i[1]))
    return 1


def trinity_info(fasta):
    """
    对trinity.fasta处理，返回转录本和基因的相关字典，*_detail：键为序列名，值为reads序列的字典；*_len: 键为序列名，值为序列长度的字典
    :param fasta: 输入文件，trinity.fasta
    """
    transcript_detail = {}
    transcript_len = {}
    gene_len = {}
    gene_detail = {}
    with open('gene.fasta', 'wb') as w:
        for seq in SeqIO.parse(fasta, "fasta"):
            transcript_detail[seq.id] = seq.seq
            transcript_len[seq.id] = len(seq.seq)
        gene_name = isoform_num(transcript_detail)
        for name in gene_name:
            unigene = {}
            for tran_name in transcript_detail.keys():
                if name in tran_name:
                    unigene[tran_name] = transcript_len[tran_name]
            max_len = max(unigene.values())
            gene_index = unigene.values().index(max_len)
            gene_len[unigene.keys()[gene_index]] = unigene.values()[gene_index]
            gene_detail[unigene.keys()[gene_index]] = transcript_detail[unigene.keys()[gene_index]]
            w.write('>%s\n%s\n' % (unigene.keys()[gene_index], transcript_detail[unigene.keys()[gene_index]]))
    return transcript_detail, transcript_len, gene_len, gene_detail


def stat(detail_dict, len_dict):
    """
    分别统计转录本，基因的信息，信息包括序列总数，碱基总数，GC含量，最长（短）转录本长度，平均长度，N50，N90
    :param detail_dict：键为序列名，值为reads序列的字典
    :param len_dict: 键为序列名，值为序列长度的字典
    """
    seq_num = len(detail_dict)
    len_value = len_dict.values()
    base_num = sum(len_value)
    GC_num = 0
    for read in detail_dict.values():
        num = read.count('C') + read.count('G')
        GC_num += num
    GC_per = float(GC_num) / base_num
    max_len = max(len_value)
    min_len = min(len_value)
    average = np.mean(len_value)
    len_value.sort(reverse=True)
    len50 = 0
    len90 = 0
    N50 = 0
    N90 = 0
    for i in len_value:
        if len50 < base_num * 0.5:
            len50 += i
            N50 = i
        elif len90 < base_num * 0.9:
            len90 += i
            N90 = i
    return seq_num, base_num, '%0.4g' % GC_per, max_len, min_len, '%0.4g' % average, N50, N90


def len_stat(len_dict, length):
    """
    传入键为序列名，值为序列长度的字典,统计每隔length长度的区间的序列数
    :param len_dict: 键为序列名，值为序列长度的字典
    :param length：步长
    """
    len_value = len_dict.values()
    max_len = max(len_value)
    if max_len % length == 0:
        n = max_len / length
    else:
        n = max_len / length + 1
    len_range = {}
    left = 1
    rigth = length
    for i in range(1, n+1):
        key = '%s-%s' % (left, rigth)
        len_range[key] = 0
        for l in len_value:
            if l >= left and l <= rigth:
                len_range[key] += 1
        left += length
        rigth += length
    read_sum = len(len_value)
    len_num = Counter(len_value).most_common()
    len_num.sort()
    return len_range, read_sum, len_num


def isoform_num(transcript_detail):
    """
    传入键为序列名，值为reads序列的字典，按照拥有的isoform（可变剪接体）数目，统计转录本的数量分布
    :param transcript_detail：键为序列名，值为reads序列的字典
    """
    names = transcript_detail.keys()
    read_name = []
    for name in names:
        read_name.append(name.split('_i')[0])
    isoform = Counter(read_name).most_common()
    gene_name = []
    i_1 = 0
    i_2 = 0
    i_3_5 = 0
    i_5_10 = 0
    i_11 = 0
    for i in isoform:
        gene_name.append(i[0])
        if i[1] == 1:
            i_1 += 1
        elif i[1] == 2:
            i_2 += 1
        elif i[1] > 2 or i[1] <= 5:
            i_3_5 += 1
        elif i[1] >= 5 or i[1] <= 10:
            i_5_10 += 1
        elif i[1] > 10:
            i_11 += 1
    with open('transcript.iso.txt', 'wb') as w:
        w.write('isoform_num\ttranscript_num\n1 isoform\t%s\n2 isoform\t%s\n3-5 isoform\t%s\n'
                '5-10 isoform\t%s\n>10 isoform\t%s' % (i_1, i_2, i_3_5, i_5_10, i_11))
    return gene_name
