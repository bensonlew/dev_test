# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.iofile import File


class KeggTableFile(File):
    """
    定义kegg_table.xls格式
    """
    def __init__(self):
        super(KeggTableFile, self).__init__()

    def check(self):
        if super(KeggTableFile, self).check():
            return True

    def get_kegg_list(self, outdir, all_list):
        with open(self.prop['path'], 'rb') as r, open(outdir + '/kofile', 'wb') as w, open(outdir + '/all_kofile', 'wb') as a:
            r.readline()
            head = '##ko KEGG Orthology\n##Method: BLAST Options: evalue <= 1e-05; rank <= 5\n##Summary: None\n\n#Query\tKO ID|KO name|Hyperlink\n'
            w.write(head)
            a.write(head)
            for line in r:
                line = line.strip('\n').split('\t')
                w.write('{}\t{}\n'.format(line[0], line[1]))
                a.write('{}\t{}\t|\t|\n'.format(line[0], line[1], line[2], line[3]))
                self.gene_list.append(line[0])
            for i in all_list:
                if i not in self.gene_list:
                    a.write('{}\tNone\n'.format(i))
