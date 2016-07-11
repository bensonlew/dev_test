# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""OTUtable格式文件类"""

from biocluster.iofile import File
import subprocess
from biocluster.config import Config
import os
import re
from biocluster.core.exceptions import FileError


class OtuTableFile(File):
    """
    OTUtable
    """
    def __init__(self):
        """
        """
        super(OtuTableFile, self).__init__()
        self.biom_path = os.path.join(Config().SOFTWARE_DIR, "program/Python/bin/")
        self.otu2shared_path = os.path.join(Config().SOFTWARE_DIR, "bioinfo/meta/scripts/otu2shared.pl")

    def get_info(self):
        """
        获取文件属性
        :return:
        """
        super(OtuTableFile, self).get_info()
        info = self.get_otuinfo()
        self.set_property("form", info[0])
        self.set_property("otu_num", info[1])
        self.set_property("sample_num", info[2])
        self.set_property("metadata", info[3])

    def check(self):
        """
        检测文件是否满足要求
        :return:
        """
        if super(OtuTableFile, self).check():
            self.get_info()
            if self.prop['form']:
                pass
            else:
                raise FileError("文件格式错误")
        return True

    def get_otuinfo(self):
        """
        获取otu表信息
        """
        form, otu_num, sample_num, metadata = True, 0, 0, ''
        with open(self.prop['path'], 'r') as f:
            heads = f.readline().rstrip().split('\t')
            colnum = len(heads)
            # if not re.match(r'#*OTU ID', heads[0]):
            #     form = False
            if colnum < 2:
                form = False
            if form:
                sample_num = colnum - 1
                if heads[colnum - 1] == 'taxonomy':
                    metadata = 'taxonomy'
                    sample_num = colnum - 2
                while 1:
                    line = f.readline().rstrip()
                    otu_num += 1
                    if not line:
                        break
        return (form, otu_num, sample_num, metadata)

    def convert_to_biom(self, biom_filepath):
        """
        转换为biom格式
        """
        # biom convert -i otu_table.txt -o otu_table.biom.rev  --table-type "otu table  --to-hdf5"
        # biom convert -i otu_taxa_table.txt -o otu_table.biom.revtax  --table-type "otu table"  --to-hdf5 --process-obs-metadata taxonomy
        cmd = "biom convert -i " + self.prop['path'] + " -o " + biom_filepath + ' --table-type \"OTU table\" --to-hdf5'
        if self.prop['metadata'] == "taxonomy":
            cmd += " --process-obs-metadata taxonomy"
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            raise Exception("biom convert 运行出错！")
        return True

    def convert_to_shared(self, shared_filepath):
        """
        转换为mothur的shared格式
        """
        # otu2shared.pl -i otutable -l 0.97 -o otu.shared
        if self.prop['metadata'] == "taxonomy":
            raise FileError(u"can not covert otutable with taxon info.")
        cmd = self.otu2shared_path + " -l 0.97 -i " + self.prop['path'] + " -o " + shared_filepath
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            raise Exception("otu2shared.pl 运行出错！")
        return True

    def del_taxonomy(self, otu_path, no_tax_otu_path):
        """
        删除taxonomy列  并返回一个字典
        """
        tax_dic = dict()
        with open(otu_path, 'rb') as r, open(no_tax_otu_path, 'wb') as w:
            line = r.next()
            line = line.rstrip('\n')
            line = re.split('\t', line)
            if line[-1] != "taxonomy":
                raise Exception("文件最后一列不为taxonomy，不需要去除taxonomy列")
            line.pop()
            w.write("\t".join(line) + "\n")
            for line in r:
                line = line.rstrip('\n')
                line = re.split('\t', line)
                tax_dic[line[0]] = line[-1]
                line.pop()
                newline = "\t".join(line)
                w.write(newline + "\n")
        return tax_dic

    def add_taxonomy(self, tax_dic, no_tax_otu_path, otu_path):
        """
        根据字典， 为一个otu表添加taxonomy列
        """
        if not isinstance(tax_dic, dict):
            raise Exception("输入的tax_dic不是一个字典")
        with open(no_tax_otu_path, 'rb') as r, open(otu_path, 'wb') as w:
            line = r.next().rstrip('\n')
            w.write(line + "\t" + "taxonomy\n")
            line = re.split('\t', line)
            if line[-1] == "taxonomy":
                raise Exception("输入文件已经含有taxonomy列")
            for line in r:
                line = line.rstrip("\n")
                w.write(line)
                line = re.split("\t", line)
                w.write("\t" + tax_dic[line[0]] + "\n")

    def complete_taxonomy(self, otu_path, complete_path):
        """
        将一个OTU表的taxnomy列补全
        """
        with open(otu_path, 'rb') as r, open(complete_path, 'wb') as w:
            line = r.next().rstrip('\n')
            w.write(line + "\n")
            line = re.split('\t', line)
            if line[-1] != "taxonomy":
                raise Exception("文件taxonomy信息缺失，请调用complete_taxonomy_by_dic方法补全taxonomy")
            for line in r:
                line = line.rstrip("\n")
                line = re.split('\t', line)
                tax = line.pop()
                info = "\t".join(line)
                new_tax = self._comp_tax(tax)
                w.write(info + "\t" + new_tax + "\n")

    def complete_taxonomy_by_dic(self, tax_dic, otu_path, complete_path):
        """
        根据字典, 添加taxnomy列并补全
        """
        if not isinstance(tax_dic, dict):
            raise Exception("输入的tax_dic不是一个字典")
        with open(otu_path, 'rb') as r, open(complete_path, 'wb') as w:
            line = r.next().rstrip('\n')
            w.write(line + "\t" + "taxonomy\n")
            line = re.split('\t', line)
            if line[-1] == "taxonomy":
                raise Exception("输入文件已经含有taxonomy列, 请调用complete_taxonomy方法补全taxonomy")
            for line in r:
                line = line.rstrip("\n")
                w.write(line)
                line = re.split('\t', line)
                tax = tax_dic[line[0]]
                new_tax = self._comp_tax(tax)
                w.write("\t" + new_tax + "\n")

    @staticmethod
    def _comp_tax(tax):
        LEVEL = {
            0: "d__", 1: "k__", 2: "p__", 3: "c__", 4: "o__",
            5: "f__", 6: "g__", 7: "s__"
        }
        tax = re.sub(r'\s', '', tax)
        cla = re.split(';', tax)
        new_cla = list()
        #  处理uncultured和Incertae_Sedis
        if re.search("(uncultured|Incertae_Sedis|norank|unidentified|Unclassified)$", cla[0], flags=re.I):
            pass
            # raise Exception("在域水平上的分类为uncultured或Incertae_Sedis或norank或unidentified或是分类水平缺失")
        # 先对输入的名字进行遍历， 当在某一水平上空着的时候， 补全
        # 例如在g水平空着的时候，补全成g__Unclassified
        for i in range(0, 8):
            if not re.search(LEVEL[i], tax):
                str_ = LEVEL[i] + "Unclassified"
                new_cla.append(str_)
            else:
                new_cla.append(cla[i])

        # 对uncultured，Incertae_Sedis，norank，unidentified进行补全
        """
        i = 0
        ori_cla = new_cla[:]
        for my_cla in new_cla:
            currentCla = re.split("__", new_cla[i])
            if i > 0:
                lastCla = re.split("__", ori_cla[i - 1])
            if re.search("(uncultured|Incertae_Sedis|norank|unidentified|Unclassified)$", new_cla[i], flags=re.I):
                if lastCla == currentCla:
                    new_cla[i] = new_cla[i] + "_" + last_classify_info
                    last_info = new_cla[i]
                else:
                    new_cla[i] = new_cla[i] + "_" + last_info

                new_cla[i] = new_cla[i] + "_" + last_info
            else:
                last_classify_info = new_cla[i]
            i += 1

        new_tax = "; ".join(new_cla)
        """

        # 先构建数据claList, 结构如下
        # [[(分类级别, 值)], [(分类级别, 值)], [(分类级别, 值)], ...]
        # 然后当这个分类级别的值为( uncultured|Incertae_Sedis|norank|unidentified|Unclassified) 之一的时候
        # 进行补全, 补全规则如下:
        # 当 当前分类级别值与上一分类级别值(初始值)相同时，跳过， 寻找再上一级别的分类级别值，直至两者不同，
        # 然后将这一分类级别extend进当前分类级别 例:[(分类级别(初始), 值),(分类级别(extend), 值)]
        # extend的分类级别可能会有多个
        # 最后将claList 还原成字符串
        claList = list()
        for i in range(0, 8):
            tmp = re.split('__', new_cla[i])
            claList.append([(tmp[0], tmp[1])])
        for i in range(1, 8):
            cla = claList[i][0][1]
            if re.search("(uncultured|Incertae_Sedis|norank|unidentified|Unclassified)", cla, flags=re.I):
                j = i - 1
                while (j >= 0):
                    last_cla = claList[j][0][1]
                    if last_cla != cla:
                        claList[i].extend(claList[j])
                        j = j - 1
                        break
                    j = j - 1
        tax_list = list()
        for i in range(0, 8):
            tmp_tax = list()
            for j in range(0, len(claList[i])):
                my_tax = "{}__{}".format(claList[i][j][0], claList[i][j][1])
                tmp_tax.append(my_tax)
            tax_list.append("_".join(tmp_tax))

        new_tax = "; ".join(tax_list)
        return new_tax

    def sub_otu_sample(self, samples, path):
        """
        从一张otu表里删除几个样本， 在删除这些样本之后，有些OTU的数目会变成0, 删除这些OTU
        """
        colnum_list = list()
        otu_dict = dict()
        new_head = list()
        if not isinstance(samples, list):
            raise Exception("samples参数必须是列表")
        with open(self.prop['path'], 'rb') as r:
            head = r.next().strip("\r\n")
            head = re.split('\t', head)
            for i in range(1, len(head)):
                if head[i] in samples:
                    colnum_list.append(i)
                    new_head.append(head[i])
            for line in r:
                line = line.strip('\r\n')
                line = re.split('\t', line)
                otu_dict[line[0]] = list()
                sum_ = 0
                for i in colnum_list:
                    otu_dict[line[0]].append(line[i])
                for num in otu_dict[line[0]]:
                    sum_ += int(num)
                if sum_ == 0:
                    del otu_dict[line[0]]
        with open(path, 'wb') as w:
            w.write("OTU ID\t")
            w.write("\t".join(new_head) + "\n")
            for otu in otu_dict:
                w.write(otu + "\t")
                w.write("\t".join(otu_dict[otu]) + "\n")

    def transposition(self, path):
        """
        转置一个otu表
        """
        file_ = list()
        with open(self.prop['path'], 'rb') as r:
            linelist = [l.strip('\r\n') for l in r.readlines()]
        for row in linelist:
            row = re.split("\t", row)
            file_.append(row)
        zip_line = zip(*file_)
        with open(path, 'wb') as w:
            for my_l in zip_line:
                w.write("\t".join(my_l) + "\n")

    def get_sample_info(self):
        """
        获取otu表中样本信息，返回样本列表
        """
        with open(self.prop['path'], 'r') as f:
            sample = f.readline().strip('\n').split('\t')
            del sample[0]
        return sample
