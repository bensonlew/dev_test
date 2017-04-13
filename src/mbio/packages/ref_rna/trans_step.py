# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'

from Bio import SeqIO
import fileinput
import re
import os
import subprocess
import urllib2
from collections import defaultdict
import regex


def step_count(fasta_file, fasta_to_txt, group_num, step, stat_out):
    """
    步长统计
    :param fasta_file: 输入的fa文件
    :param fasta_to_txt:输出的统计数据txt
    :param group_num:按照步长统计几组
    :param step:步长
    :param stat_out:统计的数据汇总信息txt（fasta_to_txt文件的汇总）
    :return:
    """
    with open(fasta_to_txt, "w") as f:
        for seq_record in SeqIO.parse(fasta_file, "fasta"):
            ID = seq_record.description.strip().split(" ")[1]
            new_trans_list = ID + "\t" + str(len(seq_record)) + "\n"
            f.write(new_trans_list)
    with open(fasta_to_txt, "r") as r, open(stat_out, "a") as w:
        sample_name = os.path.basename(fasta_file).split('.fa')[0]
        w.write(sample_name + "\n")
        trans_list = []
        amount_group = []
        element_set = set("")
        for line in r:
            line = line.strip().split("\t")
            number = line[1]
            trans_list.append(number)
        for f in trans_list:
            for i in range(group_num):
                if (int(f) >= (i * step)) and (int(f) < ((i+1) * step)):
                    amount_group.append(i)
                else:
                    pass
                    # amount_group.append(group_num+1)
                element_set.add(i)
        amount_group.sort()
        for i in element_set:
            num_statistics = amount_group.count(i)
            if i < (group_num-1):
                area_line = str(i * step) + "~" + str((i+1) * step) + "\t" + str(num_statistics) + "\n"
                w.write(area_line)
            else:
                area_line = ">" + str(i * step) + "\t" + str(num_statistics) + "\n"
                end_line = "total" + "\t" + str(len(trans_list)) + "\n"
                w.write(area_line)
                w.write(end_line)


def merged_add_code(trans_file, tmap_file, new_trans):
    def get_info_dic_from_tmap(srcfile):  # 转录本信息
        dic = dict()
        for line in fileinput.input(srcfile):
            arr = line.strip().split("\t")
            key = arr[4]  # transctipt id
            value = arr[2]
            dic[key] = value
        return dic

    candidateDic = get_info_dic_from_tmap(tmap_file)
    p = re.compile(r'transcript_id')
    fw = open(new_trans, "w+")
    for line in fileinput.input(trans_file):
        m = re.match("#.*", line)
        if not m:
            line1 = line.strip()
            arr = line1.split("\t")
            description = arr[8]
            desc_array = description.strip().split(";")
            for tag in desc_array:
                tagpair = tag.strip().split("\"")
                if len(tagpair) == 3:
                    if re.match(p, tagpair[0]):
                        if candidateDic. has_key(tagpair[1]):
                            newline1 = line1 + " class_code \"" + candidateDic[tagpair[1]] + "\";\n"
                            fw.write(newline1)

    fw.close()


def tran_exon_data(merged_gtf, gene_trans_file, trans_exon_file):
    fw1 = open(gene_trans_file, 'w')
    fw2 = open(trans_exon_file, 'w')
    gene_dic = defaultdict(int)
    trans_dic = defaultdict(int)
    for line in fileinput.input(merged_gtf):
        m = regex.search(r'^[^#]\S*\t(\S+\t){7}.*?gene_id\s+\"(\S+)\";\s+transcript_id\s+\"(\S+)\";*', line)
        if m:
            seq_type = m.captures(1)[1]
            txpt_id = m.captures(3)[0]
            gene_id = m.captures(2)[0]
            gene_dic[gene_id] += 1
            if seq_type.split("\t")[0] == "exon":
                trans_dic[txpt_id] += 1
    for key in gene_dic.keys():
        gene_line = key + "\t" + str(gene_dic[key]) + "\n"
        fw1.write(gene_line)
    for key in trans_dic.keys():
        trans_line = key + "\t" + str(trans_dic[key]) + "\n"
        fw2.write(trans_line)


def class_code_count(gtf_file, code_num_trans):
    txpt_cls_cmd = ' awk -F \';\' \'{printf $2\"\;\"$(NF-1)\"\\n\";}\'  %s | uniq ' % (gtf_file)
    cls_cmd = 'awk -F \';\' \'{printf $(NF-1)"\\n";}\' %s | awk -F \'\"\' \'{printf $2"\\n";}\'|uniq |sort |uniq' % (
    gtf_file)

    txpt_cls_content = str(subprocess.check_output(txpt_cls_cmd, shell=True)).split('\n')
    cls_content = subprocess.check_output(cls_cmd, shell=True).split('\n')

    cls_txpt_set_dic = {}
    for cls in cls_content:
        cls = cls.strip()
        if cls:
            cls_txpt_set_dic[cls] = {'txpt_set': set(), 'count': 0}

    for record in txpt_cls_content:
        m = re.search(r'\s*transcript_id\s+\"(\S+)\";\s*class_code\s+\"(\S+)\"', record.strip())
        if m:
            cls = m.group(2)
            txpt = m.group(1)
            cls_txpt_set_dic[cls]['txpt_set'].add(txpt)

    fw = open(code_num_trans, 'wb')
    for cls in cls_txpt_set_dic.keys():
        cls_txpt_set_dic[cls]['count'] = len(cls_txpt_set_dic[cls]['txpt_set'])
        newline = '{}\t{}\t{}\n'.format(cls, ','.join(cls_txpt_set_dic[cls]['txpt_set']),
                                        str(cls_txpt_set_dic[cls]['count']))
        fw.write(newline)
    fw.close()
# if __name__ == '__main__':
#     # merged_gtf = "/mnt/ilustre/users/sanger-dev/workspace/20170401/Single_assembly_module_tophat_stringtie_gene2/Assembly/output/assembly_newtranscripts/merged.gtf"
#     # output1 = "/mnt/ilustre/users/sanger-dev/workspace/20170401/Single_assembly_module_tophat_stringtie_gene2/Assembly/output/assembly_newtranscripts/1.txt"
#     # output2 = "/mnt/ilustre/users/sanger-dev/workspace/20170401/Single_assembly_module_tophat_stringtie_gene2/Assembly/output/assembly_newtranscripts/2.txt"
#     # tran_exon_data(merged_gtf, output1, output2)
#     merged_gtf = "O:\\Users\\zhaoyue.wang\\Desktop\\merged.gtf"
#     output3 = "O:\\Users\\zhaoyue.wang\\Desktop\\1.txt"
#     output4 = "O:\\Users\\zhaoyue.wang\\Desktop\\2.txt"
#     tran_exon_data(merged_gtf, output3, output4)
