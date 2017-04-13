# -*- coding: utf-8 -*-
# __author__ = fiona
# time: 2017/3/2 15:56
import re, os, regex, sys
#
# sys.path.append('/mnt/hgfs/F/code_lib/SangerBiocluster/src/biocluster')
# sys.path.append('/mnt/hgfs/F/code_lib/SangerBiocluster/src/biocluster/core')

import subprocess
from sequence_ontology import SequenceOntologyFile
from collections import defaultdict
from biocluster.iofile import File
from biocluster.core.exceptions import FileError
from biocluster.config import Config
from fasta import FastaFile

'''
检查gff标准：
https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md
seq 特征符合sequence ontology 规定 ：https://github.com/The-Sequence-Ontology/SO-Ontologies/blob/master/releases/so-xp.owl/so-simple.obo 此文件会有更新 但url地址不变
sequence_ontology_file_url = 'https://github.com/The-Sequence-Ontology/SO-Ontologies/blob/master/releases/so-xp.owl/so.obo'
'''


class Gff3File(File):
    def __init__(self):
        self._properties = {}
        self._contigs = set()
        self._pos_set = set()
        self._contig_boundary_dic = {}
        self._seq_type_set = set()
        self._strand_set = set()
        self._phase_set = set()
        self._column_number = 9
        self._contig_max_len_dic = dict()
        self._so_file = ''
        self._version = ''
        self._gtf = ''  # gff3的配套gtf路径
        self._gffread_path = ''
        self._seq_pos = dict()
        self._genes = {}
        self._mrnas = {}
        self._exons = {}
        self._cds = {}
        self._so_file = ''
        self._feature_tree = {}
        self._fasta = None

    def check(self):
        if super(Gff3File, self).check():
            return True
        else:
            raise FileError("Gff3文件路径不存在")

    def check_format(self, fa_file):
        self.check_lines()
        self.parse_and_check_simple_column(fa_file)
        self.check_logical()

    def parse_and_check_simple_column(self, fa_file):
        contig_cmd = 'grep -v \'^#\' %s |awk -F \'\\t\' \'{print $1}\' |uniq| sort  -n -r | uniq' % self.path
        self._contigs = set(
            [contig.strip() for contig in subprocess.check_output(contig_cmd, shell=True).strip().split('\n')])
        seq_type_cmd = 'grep -v \'^#\' %s |awk -F \'\\t\' \'{print $3}\' |uniq| sort  -nr | uniq' % self.path
        self._seq_type_set = set(
            [seq_type.strip() for seq_type in subprocess.check_output(seq_type_cmd, shell=True).strip().split('\n')])
        strand_cmd = 'grep -v \'^#\' %s |awk -F \'\\t\' \'{print $7}\' |uniq| sort  -n -r | uniq' % self.path
        self._strand_set = set(
            [strand.strip() for strand in subprocess.check_output(strand_cmd, shell=True).strip().split('\n')])
        phase_cmd = 'grep -v \'^#\' %s |awk -F \'\\t\' \'{print $8}\' |uniq| sort  -n -r | uniq' % self.path
        self._phase_set = set(
            [phase.strip() for phase in subprocess.check_output(phase_cmd, shell=True).strip().split('\n')])

        pos_cmd = 'grep -v \'^#\' %s |awk -F \'\\t\' \'{printf $1":"$4"-"$5"\\n"}\' |uniq |sort' % self.path
        self._pos_set = set(
            [pos.strip() for pos in subprocess.check_output(pos_cmd, shell=True).strip().split("\n")])

        attr_cmd = 'grep -v \'^#\' %s |awk -F \'\\t\' \'{print $9}\' |uniq |sort' % self.path
        self._attrs_record_set = set(
            [attrs.strip() for attrs in subprocess.check_output(attr_cmd, shell=True).strip().split('\n')])
        self.check_contigs()
        self.pos_check(fa_file)
        self.check_seq_types()
        self.check_strand()
        self.check_phase()

    def check_logical(self):
        self.phase_logical_check()
        self.attrs_format_check()
        type_id_cmd = 'grep  \'^[^#].*ID=\' %s |awk -F \'\\t\' \'{printf $3"\t"$9"\\n"}\' | uniq' % self.path
        type_id_content = [record.strip() for record in
                           subprocess.check_output(type_id_cmd, shell=True).strip().split('\n')]
        self.type_id_check(type_id_content)

    def attrs_format_check(self):
        for attr_record in self._attrs_record_set:
            if not re.match(r'^(.+?=.+?;)+.+?=.+?;*$', attr_record.strip()):
                raise Exception('column 9 must match the format: tag=value')

    def type_id_check(self, content):
        type_id_dic = defaultdict(int)
        for line in content:
            m = re.search(r'(\S+)\t.*?ID=([^;=\"\'\t,]+)?', line.strip())
            if m:
                type_id_dic[(m.group(1), m.group(2))] += 1
            else:
                continue
        for count in type_id_dic.values():
            if count > 1:
                raise Exception('ID must be Uniq among same seq types')

    def parent_relation_check(self, content):
        type_relation_set = set()
        for record in content:
            m = re.search(r'(\S+)\t.*?Parent=([^;=\"\'\t,:]+)?:([^;=\"\'\t,:]+)?;', record)
            type_relation_set.add((m.group(1), m.group(2)))
        for type_pair in type_relation_set:
            self.check_seq_types_relation(type_pair[0], type_pair[1])

    def check_contigs(self):
        for contig in self._contigs:
            if (not re.match(r'^[a-zA-Z0-9\.:^*$@!+_?-|]+$', contig)) or contig.startswith('>'):
                raise Exception('contig 错误')
            else:
                continue
        return True

    def pos_check(self, fa_path):
        self._contig_boundary_dic = dict.fromkeys(self._contigs, (0, 0))  # 注意gff文件的pos起点
        for pos in self._pos_set:
            [contig, start, end] = re.split(r'[:-]', pos)
            if not (re.match(r'^\d+$', start) and re.match(r'^\d+$', end)):
                raise Exception('illegal pos format')
            if int(start) > int(end):
                raise Exception('{} record illegal pos in gff file {}'.format(pos, self.path))
            end = int(end)
            if end > self._contig_boundary_dic[contig][1]:
                self._contig_boundary_dic[contig][1] = end
        self.set_fasta_file(fa_path)
        contig_len_dic = self._fasta.get_contig_len()
        if self._contigs != set(contig_len_dic.keys()):
            raise Exception('gff3 and fasta file contigs id set not agreed')
        for contig in contig_len_dic.keys():
            if contig_len_dic[contig] < self._contig_boundary_dic[contig][0]:
                raise Exception('illogical seq length ')

    def check_seq_types(self, target_term):
        (id_set, name_set) = self.get_offspring_so_term(target_term, 'is_a')
        illegal_term_set = self._seq_type_set - id_set.union(name_set)
        if illegal_term_set:
            raise Exception('illegal seq types')
        return True

    def parse_directives(self):
        directives_cmd = 'grep  \'^##\' %s' % self.path
        directives_content = [record.strip() for record in
                              subprocess.check_output(directives_cmd, shell=True).strip().split('\n')]
        for record in directives_content:
            v_m = re.match(r'^##gff-version\s+(\S+)$', record)
            if v_m:
                self._version = v_m.group(1)
                continue
            seq_region_m = regex.search(r'^##sequence-region\s+(\S+\s+)+$', record.strip() + ' ')
            if seq_region_m:
                self._contigs_declar = {seq_region_m.captures(1)[0].strip(): (
                    int(seq_region_m.captures(1)[1].strip()), int(seq_region_m.captures(1)[2].strip()))}
                continue

    def check_lines(self):
        for line in open(self.path):
            if re.match(r'^#', line) or re.match(r'^$', line.strip()):
                continue
            else:
                if not regex.match(r'^[^#]\S+\t(.+?\t){7}(.+?=.+?;)*(.+?=.+?)*', line.strip()):
                    raise Exception('有不合格的行')

    def set_so_file(self, value):
        if not os.path.isfile(value):
            raise Exception('so file does not exist')
        self._so_file = SequenceOntologyFile()
        self._so_file.set_path(value)
        return self._so_file

    def set_gtf_file(self, value):
        self._gtf = value
        pass

    def set_gffread_path(self, value):
        self._gffread_path = value

    def set_fasta_file(self, fa):
        self._fasta = FastaFile()
        self._fasta.set_path(fa)

    def get_offspring_so_term(self, target_term_id, relation):
        if not re.match(r'^SO:\d+$', target_term_id):
            raise Exception('illegal input so id')
        so_file = self.set_so_file(self._so_file)
        so_file.parse()
        return so_file.findAll(target_term_id, relation)

    def check_strand(self):
        for strand in self._strand_set:
            if not re.match(r'^[\.\?\-\+]$', strand):
                raise Exception('illegal strand value')

    def check_phase(self):
        for phase in self._phase_set:
            if not re.match(r'^[\.120]$', phase):
                raise Exception('illegal phase value')

    def phase_logical_check(self):
        cds_phase_cmd = 'grep -v \'^#\' %s |awk -F \'\\t\' \'$3~/CDS|cds/{print $8}\' |uniq | sort |uniq' % self.path
        cds_phase_set = set(
            [phase.strip() for phase in subprocess.check_output(cds_phase_cmd, shell=True).strip().split('\n')])
        if cds_phase_set & {0, 1, 2} != cds_phase_set:
            raise Exception('illogical phase value')

    def get_genbank_assembly_id(self):
        if self._parse_status:
            for item in self._build_info_dic.keys():
                info_macth = re.match(r'NCBI:(\S+)', self._build_info_dic[item])
                if re.match(r'.*genome-build-accession.*', item) and info_macth:
                    self._genbank_assembly_id = info_macth.group(1)
            return self._genbank_assembly_id

    def to_gtf(self):
        temp_gtf = os.path.join(os.path.dirname(self._gtf), os.path.basename(self._gtf).split('.')[0]) + '_temp.gtf'
        to_gtf_cmd = '%s -T -O -C -o %s  %s' % (self._gffread_path, temp_gtf, self.path)
        # 先加上-c参数以保证组装过程不出现错误，后期修改组装模块后取消-c参数
        subprocess.call(to_gtf_cmd, shell=True)
        gtf = open(self._gtf, 'wb')
        for line in open(temp_gtf):
            newline = re.sub(r'"(\S+?):(\S+?)";', '"\g<2>";', line)
            gtf.write(newline)
        gtf.close()

    def to_bed(self):
        self.to_gtf()
        from .gtf import GtfFile
        gtf = GtfFile()
        gtf.set_path(self._gtf)
        gtf.to_bed()


if __name__ == '__main__':
    '''
    1. trans gff3 to gtf example:
    gff3 = Gff3File()
    gff3.set_path('/mnt/hgfs/F/temp/Homo_sapiens.GRCh38.87.gff3')
    gff3.set_gtf_file('/mnt/hgfs/F/temp/Homo_sapiens.gtf')
    gff3.set_gffread_path('/home/linfang/app/cufflinks/gffread')
    gff3.to_gtf()

    2.

    '''
    gff3 = Gff3File()
    gff3.set_path('/mnt/hgfs/F/temp/Homo_sapiens.GRCh38.87.gff3')
    gff3.set_gtf_file('/mnt/hgfs/F/temp/Homo_sapiens.gtf')
    gff3.set_gffread_path('/home/linfang/app/cufflinks/gffread')
    gff3.to_gtf()
    # type_id_cmd = 'grep  \'^[^#].*ID=\' %s |awk -F \'\\t\' \'{printf $3"\t"$9"\\n"}\' | uniq' % gff3.path
    # type_id_content = [record.strip() for record in
    #                    subprocess.check_output(type_id_cmd, shell=True).strip().split('\n')]
    # gff3.type_id_check(type_id_content)
