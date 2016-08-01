# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import re


def find_position(orient, snp_position, orf_left, orf_right):
    position = "uncertain"
    if orient == "+":
        if snp_position <= orf_left:
            position = "5-UTR"
        elif snp_position > orf_right:
            position = "3-UTR"
        else:
            if (snp_position - orf_left) % 3 == 1:
                position = "first coden"
            elif (snp_position - orf_left) % 3 == 2:
                position = "second coden"
            elif (snp_position - orf_left) % 3 == 0:
                position = "third coden"
    elif orient == "-":
        if snp_position <= orf_left:
            position = "3-UTR"
        elif snp_position > orf_right:
            position = "5-UTR"
        else:
            if (snp_position - orf_left) % 3 == 1:
                position = "third coden"
            elif (snp_position - orf_left) % 3 == 2:
                position = "second coden"
            elif (snp_position - orf_left) % 3 == 0:
                position = "first coden"
    return position


def get_orf_info(bed):
    orf_info = {}
    with open(bed, 'r') as bed:
        bed.readline()
        for line in bed:
            line = line.strip().split()
            gene_name = line[0].split("_i")[0]
            if gene_name in orf_info:
                orf_info[gene_name].append([line[5], int(line[6]), int(line[7])])
            else:
                orf_info[gene_name] = [[line[5], int(line[6]), int(line[7])]]
    return orf_info


def snp_stat(snp, orf):
    orf_info = get_orf_info(orf)
    snp_type_count = {}
    snp_position_count = {"uncertain": 0}
    with open(snp, 'r') as snp, open("snp.xls", "w") as w:
        for line in snp:
            if re.match(r"Chrom", line):
                first_line = line.strip()
                first_line += "\tposition\n"
                w.write(first_line)
            else:
                write_line = line.strip()
                line = write_line.split()
                if len(line) != 19:
                    continue
                else:
                    trans_base = line[2] + line[18]
                    if trans_base in snp_type_count:
                        snp_type_count[trans_base] += 1
                    else:
                        snp_type_count[trans_base] = 0
                    if line[0] in orf_info:
                        sorted_info = sorted(orf_info[line[0]])
                        max_info = max(sorted_info)
                        snp_position = int(line[1])
                        position = find_position(max_info[0], snp_position, max_info[1], max_info[2])
                        if position in snp_position_count:
                            snp_position_count[position] += 1
                        else:
                            snp_position_count[position] = 0
                    else:
                        position = "uncertain"
                        snp_position_count["uncertain"] += 1
                    write_line = write_line + "\t" + position + "\n"
                    w.write(write_line)
        # print snp_position_count
        # print snp_type_count
        with open("snp_type_stat.xls", "w") as t, open("snp_position_stat.xls", "w") as p:
            t.write("type\tCount\n")
            t.write("A/G\t{}\n".format(snp_type_count["AG"] + snp_type_count["GA"]))
            t.write("A/T\t{}\n".format(snp_type_count["AT"] + snp_type_count["TA"]))
            t.write("A/C\t{}\n".format(snp_type_count["AC"] + snp_type_count["CA"]))
            t.write("C/T\t{}\n".format(snp_type_count["CT"] + snp_type_count["TC"]))
            t.write("C/G\t{}\n".format(snp_type_count["CG"] + snp_type_count["GC"]))
            t.write("T/G\t{}\n".format(snp_type_count["TG"] + snp_type_count["GT"]))
            p.write("snp_position\tCount\n")
            for key in snp_position_count:
                p.write("{}\t{}\n".format(key, snp_position_count[key]))
