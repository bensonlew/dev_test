# -*- coding: utf-8 -*-
# __author__ = 'qiuping'


def blastout_statistics(blast_table, evalue_path, similarity_path):
    with open(blast_table, 'rb') as f, open(evalue_path, 'wb') as e, open(similarity_path, 'wb') as s:
        count_evalue = [0] * 6
        count_similar = [0] * 5
        f.readline()
        for line in f:
            line = line.strip('\n').split('\t')
            evalue = float(line[1])
            similarity = float(line[4])
            if not evalue:
                count_evalue[0] += 1
            elif 0 < evalue <= 1e-30:
                count_evalue[1] += 1
            elif 1e-30 < evalue <= 1e-20:
                count_evalue[2] += 1
            elif 1e-20 < evalue <= 1e-10:
                count_evalue[3] += 1
            elif 1e-10 < evalue <= 1e-5:
                count_evalue[4] += 1
            else:
                count_evalue[5] += 1
            if 0 <= similarity <= 20:
                count_similar[0] += 1
            elif 20 < similarity <= 40:
                count_similar[1] += 1
            elif 40 < similarity <= 60:
                count_similar[2] += 1
            elif 60 < similarity <= 80:
                count_similar[3] += 1
            else:
                count_similar[4] += 1
        e.write("evlaue_interval\tnum_hits\n")
        e.write('0\t{}\n0 1e-30\t{}\n1e-30 1e-20\t{}\n1e-20 1e-10\t{}\n1e-10 1e-5\t{}\n1e-5 inf\t{}\n'.format(count_evalue[0], count_evalue[1], count_evalue[2], count_evalue[3], count_evalue[4], count_evalue[5]))
        s.write('similarity_interval\tnum_hits\n')
        for i in range(len(count_similar)):
            s.write('{}%-{}%\t{}\n'.format(i * 20, (i + 1) * 20, count_similar[i]))
