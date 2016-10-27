# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.iofile import File
from biocluster.core.exceptions import FileError


class GeneListFile(File):
    """
    gene名字列表文件
    """
    def __init__(self):
        super(GeneListFile, self).__init__()

    def get_info(self):
        """
        获取文件属性
        """
        super(GeneListFile, self).get_info()
        table_info = self.get_gene_list()
        self.set_property('gene_list', table_info[0])
        self.set_property('gene_num', table_info[1])

    def check(self):
        if super(GeneListFile, self).check():
            self.get_info()
            return True

    def get_one_col(self, output):
        with open(self.prop['path'], 'rb') as r, open(output, 'wb') as w:
            i = 0
            for f in r:
                i += 1
                if i == 1:
                    pass
                else:
                    w.write(f.split('\t')[0] + '\n')

    def get_gene_list(self):
        gene_list = []
        with open(self.prop['path'], 'rb') as r:
            aline = r.readline().strip('\n').split('\t')
            if len(aline) == 1:
                gene_list.append(aline[0])
            for line in r:
                line = line.strip('\n').split('\t')
                gene_list.append(line[0])
        length = len(gene_list)
        return gene_list, length
