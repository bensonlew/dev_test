#!/usr/bin/python
import os
from Bio import SeqIO


def step_count(fasta_file,fasta_to_txt,group_num,step,stat_out):
    with open(fasta_to_txt, "w") as f:
        for seq_record in SeqIO.parse(fasta_file, "fasta"):
            ID = seq_record.description.strip().split(" ")[1]
            new_trans_list = ID + "\t" + str(len(seq_record)) + "\n"
            f.write(new_trans_list)
    with  open(fasta_to_txt, "r") as r, open(stat_out, "a") as w:
        sample_name = os.path.basename(fasta_file).split('.fa')[0]
        w.write(sample_name+ "\n")
        i=0
        trans_list=[]
        amount_group = []
        element_set = set("")
        for line in r:
            line = line.strip().split("\t")
            number = line[1]
            trans_list.append(number)
        for f in trans_list:
            for i in range(group_num):
                if int(f) >= (i * step) and int(f) < ((i+1) * step):
                    amount_group.append(i)
                else :
                    amount_group.append(group_num+1)
                element_set.add(i)
        amount_group.sort()
        for i in element_set:
            num_statistics=amount_group.count(i)
            if i < (group_num-1):
                area_line = str(i * step) + "~" + str((i+1) * step) + "\t" + str(num_statistics) + "\n"
                w.write(area_line)
            else:
                area_line = ">" + str(i * step) + "\t" + str(num_statistics) + "\n"
                w.write(area_line)
