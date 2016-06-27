# -*- coding: utf-8 -*-
# __author__ = 'xuting'
# __version__ = 'v1.0'
# __last_modified__ = '20160623'
from __future__ import division
import argparse
import math


parser = argparse.ArgumentParser(description='统计一个fastq序列的序列信息')
parser.add_argument('-p', '--phred', help='phred of the input fastq, default:33')
parser.add_argument('-i', '--input', help='input fastq file', required=True)
parser.add_argument('-o', '--output', help='output stat file', required=True)
args = vars(parser.parse_args())

if args['phred']:
    phred = int(args['phred'])
else:
    phred = 33
infile = args['input']
outfile = args['output']

try:
    with open(infile, 'rb') as r:
        pass
except IOError:
    raise IOError('无法打开输入的fastq文件，请检查文件路径是否正确')

try:
    with open(outfile, 'wb') as w:
        pass
except IOError:
    raise IOError('无法生成输出文件，请检查是否有输出路径的写入权限')

with open(infile, 'rb') as r:
    line = r.next()
    if line[0] != "@":
        raise ValueError("fastq 文件格式不正确")
    line = r.next()
    line = r.next()
    if line[0] != '+':
        raise ValueError("fastq 文件格式不正确")


totalBase = 0
q20Base = 0
q30Base = 0
count = 0
readWithNs = 0
ABase = 0
TBase = 0
CBase = 0
GBase = 0
NBase = 0
totalQuality = 0
with open(infile, 'rb') as r:
    for line in r:
        count += 1
        if count % 10000 == 0:
            print "processing seq " + str(count)
        line = r.next().rstrip("\r\n")
        length = len(line)
        totalBase += length
        ABase += line.count("A")
        ABase += line.count("a")
        TBase += line.count("T")
        TBase += line.count("t")
        CBase += line.count("C")
        CBase += line.count("c")
        GBase += line.count("G")
        GBase += line.count("g")
        NBase += line.count("N")
        NBase += line.count("n")
        if "N" in line or "n" in line:
            readWithNs += 1
        line = r.next()
        line = r.next().rstrip('\r\n')
        for i in xrange(length):
            value = ord(line[i])
            totalQuality += value - phred
            if value - phred >= 20:
                q20Base += 1
            if value - phred >= 30:
                q30Base += 1


readsWithNRate = readWithNs / totalBase
ARate = ABase / totalBase
TRate = TBase / totalBase
CRate = CBase / totalBase
GRate = GBase / totalBase
NRate = NBase / totalBase
q20Rate = q20Base / totalBase
q30Rate = q30Base / totalBase
errorRate = math.pow(10.0, (totalQuality / totalBase) * -0.1) * 100

with open(outfile, 'wb') as w:
    header = "total_reads\ttotal_bases\tTotal_Reads_with_Ns\tN_Reads%\tA%\t%T\t%C\t%G\t%N\tError%\tQ20%\tQ30%\tGC%"
    str_ = "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(count, totalBase, readWithNs, readsWithNRate, ARate, TRate, CRate, GRate, NRate, errorRate, q20Rate, q30Rate, CRate + GRate)
    w.write(header + "\n")
    w.write(str_)
