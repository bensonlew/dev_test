#!/mnt/ilustre/users/sanger/app/Python/bin/python
# -*- coding: utf-8 -*-
# __author__ = "guhaidong"
import os,re


def mask_taxon(abund, new_abund):
    """
    将注释信息（丰度表第一列信息转换为"name+数字"形式）
    :param abund: 输入的原始丰度文件
    :param new_abund: 输出掩蔽掉原有注释信息的丰度文件
    :return:taxon_to_name[new_name] = name，并返回文件taxon_to_name.xls
    """
    taxon_to_name = {}
    out_path = os.path.dirname("new_abund")
    taxon_to_name_file = os.path.join(out_path, "taxon_to_name.xls")
    with open(abund, "r") as f, open(new_abund, "w") as w, open(taxon_to_name_file, "w") as nf:
        first_line = f.readline()
        w.write(first_line)
        n = 1
        for line in f:
            line = line.split("\t")
            name = line[0]
            new_name = "name" + str(n)
            nf.write(new_name + "\t" + name + "\n")
            taxon_to_name[new_name] = name
            n += 1
            new_line = new_name + "\t" + "\t".join(line[1:])
            w.write(new_line)
    return taxon_to_name

'''
def dashrepl(taxon_to_name, matchobj):
    return taxon_to_name[matchobj.groups()[0]]

def add_taxon(taxon_to_name, old_result, taxon_result):
    """
    用新名称对应就名称的字典，将结果中的“name”转换为原名称
    :param taxon_to_name: mask_taxon生成的字典
    :param old_result: 输入的分析结果文件，用“name”替换掉注释信息作为列名
    :return:
    """
    with open(old_result, "r") as f, open(taxon_result, "w") as w:
        line = f.readline().strip()
        new_line = re.sub(r"(name\d+)", dashrepl(taxon_to_name), line)
        w.write(new_line)
'''