# -*- coding: utf-8 -*-
# __author__ = 'zengjing'


def blastout_statistics(blast_table, evalue_path, similarity_path):
    with open(blast_table, 'rb') as f, open(evalue_path, 'wb') as e, open(similarity_path, 'wb') as s:
        count_evalue = [0] * 6
        count_similar = [0] * 5
        f.readline()
        for line in f:
            line = line.strip('\n').split('\t')
            evalue = float(line[1])
            similarity = float(line[4])
            if 0 <= evalue <= 1e-30:
                count_evalue[0] += 1
            elif 1e-30 < evalue <= 1e-20:
                count_evalue[1] += 1
            elif 1e-20 < evalue <= 1e-10:
                count_evalue[2] += 1
            elif 1e-10 < evalue <= 1e-5:
                count_evalue[3] += 1
            elif 1e-5 < evalue <= 1e-3:
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
        e.write('[0, 1e-30]\t{}\n'.format(count_evalue[0]))
        e.write('(1e-30, 1e-20]\t{}\n'.format(count_evalue[1]))
        e.write('(1e-20, 1e-10]\t{}\n'.format(count_evalue[2]))
        e.write('(1e-10, 1e-5]\t{}\n'.format(count_evalue[3]))
        e.write('(1e-5, 1e-3]\t{}\n'.format(count_evalue[4]))
        # e.write('(>1e-5)\t{}\n'.format(count_evalue[4]))
        s.write('similarity_interval\tnum_hits\n')
        s.write('[0%, 20%]\t{}\n'.format(count_similar[0]))
        s.write('(20%, 40%]\t{}\n'.format(count_similar[1]))
        s.write('(40%, 60%]\t{}\n'.format(count_similar[2]))
        s.write('(60%, 80%]\t{}\n'.format(count_similar[3]))
        s.write('(80%, 100%]\t{}\n'.format(count_similar[4]))
