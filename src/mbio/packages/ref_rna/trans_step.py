# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'

from Bio import SeqIO
import fileinput
import re
import os
import subprocess
import urllib2
from collections import defaultdict


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


def count_trans_or_exons(input_file, count_file):
    """
    统计对应关系的信息
    :param input_file:第一列：基因id，第二列：对应的所有转录本的id
    :param count_file:第一列：基因（转录本）对应的转录本（外显子）的个数，第二列：这样的基因（转录本）有多少个，第三列：list,分别是哪些基因（转录本）id
    """
    f = open(count_file, 'w')
    dic = {}
    list_set = set()
    for line in fileinput.input(input_file):
        lines = line.strip().split("\t")
        id_name = lines[1].strip("[").strip("]").strip().split(",")
        dic[lines[0]] = len(id_name)
        list_set.add(len(id_name))
    for i in list_set:
        value = 0
        ids = []
        for key in dic.keys():
            if dic[key] == i:
                value += 1
                ids.append(key)
            else:
                pass
        new_line = str(i) + "\t" + str(value) + "\t" + str(ids) + "\n"
        f.write(new_line)
    f.close()


def gene_trans_data(merged_gtf, gene_trans, final_file):
    """
    统计gtf文件中每个基因所对应的转录本
    :param merged_gtf: gtf文件
    :param gene_trans: 第一列：基因id，第二列：对应的所有转录本的id
    :param final_file: 第一列：基因（转录本）对应的转录本（外显子）的个数，第二列：这样的基因（转录本）有多少个，第三列：list,分别是哪些基因（转录本）id
    :return:
    """
    f = open(gene_trans, 'w')
    gene_tran = {}
    for line in fileinput.input(merged_gtf):
        genes = line.strip().split("\t")[-1].split(";")[0]
        tran = line.strip().split("\t")[-1].split(";")[1]
        gene_id = genes.strip().split('"')[1]
        tran_id = tran.strip().split('"')[1]
        if gene_id in gene_tran.keys():
            gene_tran[gene_id].append(tran_id)
        else:
            gene_tran[gene_id] = [tran_id]
    for key in gene_tran.keys():
        new_line = key + "\t" + str(gene_tran[key]) + "\n"
        f.write(new_line)
    count_trans_or_exons(gene_trans, final_file)
    f.close()


def tran_exon_data(merged_gtf, trans_exon, final_file):
    f = open(trans_exon, 'w')
    tran_exon = {}
    for line in fileinput.input(merged_gtf):
        tran = line.strip().split("\t")[-1].split(";")[1]
        tran_id = tran.strip().split('"')[1]
        exon_start = line.strip().split("\t")[3]
        exon_end = line.strip().split("\t")[4]
        exon = exon_start + "~" + exon_end
        if tran_id in tran_exon.keys():
            tran_exon[tran_id].append(exon)
        else:
            tran_exon[tran_id] = [exon]
    for key in tran_exon.keys():
        new_line = key + "\t" + str(tran_exon[key]) + "\n"
        f.write(new_line)
    count_trans_or_exons(trans_exon, final_file)
    f.close()


def class_code_count(gtf_file, code_num_trans):
    txpt_cls_cmd = ' awk -F \';\' \'{printf $2\"\;\"$(NF-2)\"\\n\";}\'  %s | uniq ' % (gtf_file)
    cls_cmd = 'awk -F \';\' \'{printf $(NF-1)"\\n";}\' %s | awk -F \'\"\' \'{printf $2"\\n";}\'|uniq |sort |uniq' % (
    gtf_file)

    txpt_cls_content = str(subprocess.check_output(txpt_cls_cmd, shell=True)).split('\n')
    cls_content = subprocess.check_output(cls_cmd, shell=True).split('\n')
    print(cls_content)

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
if __name__ == '__main__':
    merged_gtf = "/mnt/ilustre/users/sanger-dev/workspace/20170331/Single_assembly_module_tophat_stringtie_gene2/Assembly/output/assembly_newtranscripts/merged.gtf"
    output = "/mnt/ilustre/users/sanger-dev/workspace/20170331/Single_assembly_module_tophat_stringtie_gene2/Assembly/output/assembly_newtranscripts/1.txt"
    class_code_count(merged_gtf, output)

