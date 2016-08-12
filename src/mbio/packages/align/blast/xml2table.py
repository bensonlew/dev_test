# -*- coding: utf-8 -*-
# __author__ = 'sheng.he'

import os
import re
from Bio.Blast import NCBIXML


default_header = ['Score', 'E-Value', 'HSP-Len', 'Identity-%', 'Similarity-%', 'Query-Name', 'Q-Len', 'Q-Begin',
                  'Q-End', 'Q-Frame', 'Hit-Name', 'Hit-Len', 'Hsp-Begin', 'Hsp-End', 'Hsp-Frame', 'Hit-Description']

all_values = ['Score', 'E-Value', 'HSP-Len', 'Identity-%', 'Similarity-%', 'Identity', 'Positives',
              'Query-Name', 'Q-Len', 'Q-Begin', 'Q-End', 'Q-Frame', 'Hit-Name', 'Hit-Len', 'Hsp-Begin',
              'Hsp-End', 'Hsp-Frame', 'Hit-Description', 'Q-Strand', 'Hsp-Strand']


def xml2table(xml_fp, table_out, header=None):
    if header:
        for i in header:
            if i not in all_values:
                raise Exception('无法获取的值:{}\n可用的值:{}'.format(i, '\t'.join(all_values)))
    else:
        header = default_header
    if not os.path.isfile(xml_fp):
        raise Exception('输入xml文件不存在:{}'.format(xml_fp))
    with open(xml_fp) as f, open(table_out, 'w') as w:
        w.write('\t'.join(header) + '\n')
        records = NCBIXML.parse(f)
        values = {i: 'N/A' for i in all_values}
        for rec in records:
            query = re.split(' ', rec.query, maxsplit=1)[0]
            for align in rec.alignments:
                for hsp in align.hsps:
                    one_hsp = values.copy()
                    one_hsp['Query-Name'] = query
                    one_hsp['Hit-Name'] = align.hit_id
                    one_hsp['Hit-Description'] = align.hit_def
                    one_hsp['Score'] = str(hsp.score)
                    one_hsp['E-Value'] = str(hsp.expect)
                    one_hsp['HSP-Len'] = str(hsp.align_length)
                    one_hsp['Identity'] = str(hsp.identities)
                    one_hsp['Positives'] = str(hsp.positives)
                    one_hsp['Q-Len'] = str(rec.query_length)
                    one_hsp['Q-Begin'] = str(hsp.query_start)
                    one_hsp['Q-End'] = str(hsp.query_end)
                    one_hsp['Q-Frame'] = str(hsp.frame[0])
                    one_hsp['Hit-Len'] = str(align.length)
                    one_hsp['Hsp-Begin'] = str(hsp.sbjct_start)
                    one_hsp['Hsp-End'] = str(hsp.sbjct_end)
                    one_hsp['Hsp-Frame'] = str(hsp.frame[1])
                    one_hsp['Q-Strand'] = str(hsp.strand[0])
                    one_hsp['Hsp-Strand'] = str(hsp.strand[1])
                    one_hsp['Identity-%'] = str(round(float(hsp.identities) / hsp.align_length, 3) * 100)
                    one_hsp['Similarity-%'] = str(round(float(hsp.positives) / hsp.align_length, 3) * 100)
                    line = list()
                    for i in header:
                        line.append(one_hsp[i])
                    w.write('\t'.join(line) + '\n')
    return table_out

if __name__ == '__main__':  # for test
    # xml2table('C:\\Users\\sheng.he.MAJORBIO\\Desktop\\annotation\\annotation\\NR\\transcript.fa_vs_nr.xml',
            #   'C:\\Users\\sheng.he.MAJORBIO\\Desktop\\blast_test.xls', ['Score', 'E-Value', 'HSP-Len', 'Positives'])
    a = 'C:\\Users\\sheng.he.MAJORBIO\\Desktop\\Trinity_vs_nr.xml'
    b = 'C:\\Users\\sheng.he.MAJORBIO\\Desktop\\blast_test_1.xls'
    xml2table(a, b)
