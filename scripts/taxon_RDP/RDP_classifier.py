# -*- coding: utf-8 -*-
# __author__ = 'sheng.he'
# last_modified:20160523
# 脚本的主要代码来自qiime/assign_taxonomy.py以及其使用到的相关脚本


import argparse
import re
import os
import subprocess
from train_taxon_by_RDP import RdpTrainingSet
from biocluster.config import Config


def get_argus():
    parse = argparse.ArgumentParser(
        description='''序列RDP分类脚本''',
        prog='RDP_classifier.py')
    parse.add_argument('-p', '--properties', required=True, help='训练好的的分类库文件 (necessary)\n\
                                                                  如果没有，请使用train_taxon_by_RDP.py对库文件进行训练')
    parse.add_argument('-q', '--quary_fasta', required=True, help='分类文件的fasta序列 (necessary)')
    parse.add_argument('-o', '--output_dir', required=True, help='输出文件夹 (necessary)')
    parse.add_argument('-c', '--confidence', default=0.7, type=float, help='最小分类置信度')
    parse.add_argument('-m', '--max_memory', default=10, type=int, help='最大内存:数字(单位为g)')
    args = parse.parse_args()
    if not os.path.isfile(args.properties) or not os.path.isfile(args.quary_fasta):
        raise IOError('{}或者{} 文件不存在或者不是文件'.format(args.quary_fasta, args.properties))
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)
        print('INFO:输出目录不存在，已创建目录')
    if not 1 >= args.confidence > 0:
        raise Exception('置信度设定必须在(0,1]')
    return args.properties, args.quary_fasta, args.output_dir.rstrip('/'), args.confidence, args.max_memory


def run_classifier(quary, properties, max_memory, outputfile, process_out):
    if 'RDP_JAR_PATH' in os.environ:
        RDP_fp = os.getenv('RDP_JAR_PATH')
    else:
        RDP_fp = '{}/dist/classifier.jar'.format(os.path.dirname(os.path.realpath(__file__)))
        if not os.path.exists(RDP_fp):
            raise Exception('RDP文件的默认路径不存在，请在环境变量中添加RDP_JAR_PATH')
    max_memory = str(max_memory) + 'g'
    java_path = Config().SOFTWARE_DIR + '/program/sun_jdk1.8.0/bin/java'
    cmd = java_path + ' -Xmx{} -jar {} classify -t {} -o {} {} -f allrank'.format(max_memory, RDP_fp, properties,
                                                                                  outputfile, quary)
    output = subprocess.check_output(cmd, shell=True)
    open(process_out, 'w').write(output)


def parse_rdp_exception(line):
    if line.startswith('ShortSequenceException'):
        matchobj = re.search('recordID=(\S+)', line)
        if matchobj:
            rdp_id = matchobj.group(1)
            return ('ShortSequenceException', rdp_id)
    return None


def parse_rdp_assignment(line):
    """Returns a list of assigned taxa from an RDP classification line."""
    toks = line.strip().split('\t')
    seq_id = toks.pop(0)
    direction = toks.pop(0)
    if ((len(toks) % 3) != 0):
        raise ValueError(
            "Expected assignments in a repeating series of (rank, name, "
            "confidence), received %s" % toks)
    assignments = []
    # Fancy way to create list of triples using consecutive items from
    # input.  See grouper function in documentation for itertools for
    # more general example.
    itoks = iter(toks)
    for taxon, rank, confidence_str in zip(itoks, itoks, itoks):
        if not taxon:
            continue
        assignments.append((taxon.strip('"'), rank, float(confidence_str)))
    return seq_id, direction, assignments


def get_rdp_lineage(rdp_taxa, min_confidence):
    lineage = []
    obs_confidence = 1.0
    for taxon, rank, confidence in rdp_taxa:
        if confidence >= min_confidence:
            obs_confidence = confidence
            lineage.append(taxon)
        else:
            break
    return lineage, obs_confidence


def format_result(result_file, stdout, min_confidence, output_fp):
    assignments = {}
    stdout_file = open(stdout)
    for line in stdout_file:
        excep = parse_rdp_exception(line)
        if excep is not None:
            _, rdp_id = excep
            assignments[rdp_id] = ('Unassignable', 1.0)
    result = open(result_file)
    for line in result:
        rdp_id, direction, taxa = parse_rdp_assignment(line)
        if taxa[0][0] == "Root":
            taxa = taxa[1:]
        lineage, confidence = get_rdp_lineage(taxa, min_confidence)
        if lineage:
            assignments[rdp_id] = (';'.join(lineage), confidence)
        else:
            assignments[rdp_id] = ('d__Unclassified', 1.0)
    try:
        output_file = open(output_fp, 'w')
    except OSError:
        raise OSError("Can't open output file for writing: %s" % output_fp)
    for seq_id, assignment in assignments.items():
        lineage, confidence = assignment
        output_file.write('%s\t%s\t%1.3f\n' % (seq_id, lineage, confidence))
    output_file.close()
    return None


def main():
    properties, quary, out_dir, confidence, max_memory = get_argus()
    run_classifier(quary=quary, properties=properties, max_memory=max_memory,
                   outputfile=out_dir + '/classified.results',
                   process_out=out_dir + '/classified.out')
    format_result(out_dir + '/classified.results', out_dir + '/classified.out', confidence,
                  out_dir + '/format_classified_with_confidence.txt')
    RdpTrainingSet.fix_output_file(out_dir + '/format_classified_with_confidence.txt')


if __name__ == '__main__':
    main()
