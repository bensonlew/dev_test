# # -*- coding: utf-8 -*-
# # __author__ = 'qindanhua'
from __future__ import division


def fastq_stat(qual_stat):
    with open(qual_stat, 'rb') as f, open('qual.stat.base', 'wb') as b, open('qual.stat.err', 'wb') as e, \
            open('qual.stat.qaul', 'wb') as q:
        b.write('position\tA\tT\tC\tG\tN\n')
        e.write('position\terror\n')
        q.write('position\tLW\tQ1\tmed\tQ3\tRW\n')
        f.readline()
        for line in f:
            ln = line.strip().split('\t')
            A_base = int(ln[12])/int(ln[17]) * 100
            T_base = int(ln[15])/int(ln[17]) * 100
            C_base = int(ln[13])/int(ln[17]) * 100
            G_base = int(ln[14])/int(ln[17]) * 100
            N_base = int(ln[16])/int(ln[17]) * 100
            err = 10 ** (float(ln[5])/(-10)) * 100
            b_line = '{}\t{}\t{}\t{}\t{}\t{}\n'.format(ln[0], A_base, T_base, C_base, G_base, N_base)
            e_line = '{}\t{}\n'.format(ln[0], err)
            q_line = '{}\t{}\t{}\t{}\t{}\t{}\n'.format(ln[0], ln[10], ln[6], ln[7], ln[8], ln[11])
            b.write(b_line)
            q.write(q_line)
            e.write(e_line)

# path = 'C:\Users\danhua.qin\Desktop/test_file/rna/1.qual.stat'
# fastq_stat(path)
