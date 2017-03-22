# -*- coding: utf-8 -*-
# __author__ = fiona
# time: 2017/3/2 15:56

import re, os, Bio, argparse, sys, fileinput, urllib2,logging
import Bio
import subprocess
from bs4 import BeautifulSoup
from sequence_ontology_file import SequenceOntologyFile
from file import File
from fasta_file import FastaFile
from gtf_file import GtfFile

'''
检查gff标准：
https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md
seq 特征符合sequence ontology 规定 ：https://github.com/The-Sequence-Ontology/SO-Ontologies/blob/master/releases/so-xp.owl/so-simple.obo 此文件会有更新 但url地址不变

'''

sequence_ontology_file_url = 'https://github.com/The-Sequence-Ontology/SO-Ontologies/blob/master/releases/so-xp.owl/so-simple.obo'


class Gff3File(File):
	def __init__(self):
		self._contigs = set()
		self._fasta = FastaFile()
		self._column_number = 9
		self._gene_lst = []
		self._transcript_lst = []
		self._exon_lst = []
		self.exon_info = dict()
		self._hierarchy_dic = dict()
		self._contig_max_len_dic = dict()
		self._build_info_dic = dict()
		self._gtf = GtfFile()
		self._version = ''
		self._genbank_assembly_id = ''
		self._parse_status = False
		self._check = True
		self._seq_features = set()
		self._seq_ontology = set()
		self._seq_pos = dict()
	
	def check(self):
		super(Gff3File, self).check()
		self.parse()
	
	# 判断
	error_format = 'Line {current_line_num}: {error_type}: {message}\n-> {line}'
	def add_line_error(self, line_dic, error_info, log_level=logging.ERROR):
		"""Helper function to record and log an error message
		:param line_dic: dict
		:param error_info: dict
		:param logger:
		:param log_level: int
		:return:
		"""
		if not error_info: return
		try:
			line_dic['line_errors'].append(error_info)
		except KeyError:
			line_dic['line_errors'] = [error_info]
		except TypeError:  # no line_dic
			pass
		try:
			self.logger.log(log_level, Gff3File.error_format.format(current_line_num=line_dic['line_index'] + 1,
			                                                    error_type=error_info['error_type'],
			                                                    message=error_info['message'],
			                                                    line=line_dic['line_raw'].rstrip()))
		except AttributeError:  # no logger
			pass
	
	
	def parse(self):
		
		'''
		
		The strand of the feature. + for positive strand (relative to the landmark),
		  for minus strand,
		and . for features that are not stranded.
		In addition, ? can be used for features whose strandedness is relevant, but unknown.
		:return:
		'''
		
		
		
		valid_strand = set(('+', '-', '.', '?'))
		'''
		indicating the number of bases that should be removed
		from the beginning of this feature
		to reach the first base of the next codon.
		a phase of "0" indicates that the next codon begins at the first base of the region described by the current line,
		a phase of "1" indicates that the next codon begins at the second base of this region,
		and a phase of "2" indicates that the codon begins at the third base of this region.
		'''
		valid_phase = set((0, 1, 2))  # ?
		'''
		已约定的属性标签
		'''
		line_no = 0
		multi_defined_feature_attributes = set(
			('ID', 'Name', 'Alias', 'Parent', 'Target', 'Gap', 'Derives_from', 'Note', 'Db'))
		line_dic = {
			'line_type':'',
			'line_index':line_no-1,
			'raw_line':'',
			'parents':[],
			'children':[],
			'summary':''
					}
		if self.path() and super(Gff3File, self).check():
			with open(self._path) as fr:
				for line in fr:
					line_strip = line.strip()
					
					#检查每一行起始位置是否有空格，有空格则更新错误日志
					if line_strip != line[:len(line_strip)]:
						self.add_line_error(line_dic, {'message': 'White chars not allowed at the start of a line',
						                                'error_type': 'FORMAT', 'location': ''})
					#检查第一行是否声明gff文件的版本
					if line_no == 1 and not line_strip.startswith('##gff-version'):
						self.add_line_error(line_dic, {'message': '"##gff-version" missing from the first line',
						                                'error_type': 'FORMAT', 'location': ''})
						
					#标记空行
					if len(line_strip) == 0:
						line_dic['line_type'] = 'blank'
						continue
					#解析以#为起始的行
					if line_strip.startswith('##'):
						line_dic['line_type'] = 'summary'
						if line_strip.startswith('##sequence-region'):
							# ##sequence-region seqid start end
							# This element is optional, but strongly encouraged because it allows parsers to perform bounds checking on features.
							# only one ##sequence-region summary may be given for any given seqid
							# all features on that landmark feature (having that seqid) must be contained within the range defined by that ##sequence-region diretive. An exception to this rule is allowed when a landmark feature is marked with the Is_circular attribute.
							line_dic['summary'] = '##sequence-region'
							tokens = line_strip.split()[1:]
							if len(tokens) != 3:
								self.add_line_error(line_dic, {
									'message': 'Expecting 3 fields, got %d: %s' % (len(tokens) - 1, repr(tokens[1:])),
									'error_type': 'FORMAT', 'location': ''})
							if len(tokens) > 0:
								line_dic['seqid'] = tokens[0]
								# check for duplicate ##sequence-region seqid
								if [True for d in lines if ('summary' in d and d[
									'summary'] == '##sequence-region' and 'seqid' in d and d['seqid'] == line_dic[
									'seqid'])]:
									self.add_line_error(line_dic, {
										'message': '##sequence-region seqid: "%s" may only appear once' % line_dic[
											'seqid'], 'error_type': 'FORMAT', 'location': ''})
								try:
									all_good = True
									try:
										line_dic['start'] = int(tokens[1])
										if line_dic['start'] < 1:
											self.add_line_error(line_dic, {
												'message': 'Start is not a valid 1-based integer coordinate: "%s"' %
												           tokens[1], 'error_type': 'FORMAT', 'location': ''})
									except ValueError:
										all_good = False
										self.add_line_error(line_dic, {
											'message': 'Start is not a valid integer: "%s"' % tokens[1],
											'error_type': 'FORMAT', 'location': ''})
										line_dic['start'] = tokens[1]
									try:
										line_dic['end'] = int(tokens[2])
										if line_dic['end'] < 1:
											self.add_line_error(line_dic, {
												'message': 'End is not a valid 1-based integer coordinate: "%s"' %
												           tokens[2], 'error_type': 'FORMAT', 'location': ''})
									except ValueError:
										all_good = False
										self.add_line_error(line_dic,
										                    {'message': 'End is not a valid integer: "%s"' % tokens[2],
										                     'error_type': 'FORMAT', 'location': ''})
										line_dic['start'] = tokens[2]
									# if all_good then both start and end are int, so we can check if start is not less than or equal to end
									if all_good and line_dic['start'] > line_dic['end']:
										self.add_line_error(line_dic,
										                    {'message': 'Start is not less than or equal to end',
										                     'error_type': 'FORMAT', 'location': ''})
								except IndexError:
									pass
						elif line_strip.startswith('##gff-version'):
							# The GFF version, always 3 in this specification must be present, must be the topmost line of the file and may only appear once in the file.
							line_dic['summary'] = '##gff-version'
							# check if it appeared before
							if [True for d in lines if ('summary' in d and d['summary'] == '##gff-version')]:
								self.add_line_error(line_dic, {'message': '##gff-version missing from the first line',
								                                'error_type': 'FORMAT', 'location': ''})
							tokens = line_strip.split()[1:]
							if len(tokens) != 1:
								self.add_line_error(line_dic, {
									'message': 'Expecting 1 field, got %d: %s' % (len(tokens) - 1, repr(tokens[1:])),
									'error_type': 'FORMAT', 'location': ''})
							if len(tokens) > 0:
								try:
									line_dic['version'] = int(tokens[0])
									if line_dic['version'] != 3:
										self.add_line_error(line_dic,
										                    {'message': 'Version is not "3": "%s"' % tokens[0],
										                     'error_type': 'FORMAT', 'location': ''})
								except ValueError:
									self.add_line_error(line_dic,
									                    {'message': 'Version is not a valid integer: "%s"' % tokens[0],
									                     'error_type': 'FORMAT', 'location': ''})
									line_dic['version'] = tokens[0]
						elif line_strip.startswith('###'):
							# This summary (three # signs in a row) indicates that all forward references to feature IDs that have been seen to this point have been resolved.
							line_dic['summary'] = '###'
						elif line_strip.startswith('##FASTA'):
							# This notation indicates that the annotation portion of the file is at an end and that the
							# remainder of the file contains one or more sequences (nucleotide or protein) in FASTA format.
							line_dic['summary'] = '##FASTA'
							self.logger.info('Reading embedded ##FASTA sequence')
							self.fasta_embedded, count = fasta_file_to_dict(gff_fp)
							self.logger.info('%d sequences read' % len(self.fasta_embedded))
						elif line_strip.startswith('##feature-ontology'):
							# ##feature-ontology URI
							# This summary indicates that the GFF3 file uses the ontology of feature types located at the indicated URI or URL.
							line_dic['summary'] = '##feature-ontology'
							tokens = line_strip.split()[1:]
							if len(tokens) != 1:
								self.add_line_error(line_dic, {
									'message': 'Expecting 1 field, got %d: %s' % (len(tokens) - 1, repr(tokens[1:])),
									'error_type': 'FORMAT', 'location': ''})
							if len(tokens) > 0:
								line_dic['URI'] = tokens[0]
						elif line_strip.startswith('##attribute-ontology'):
							# ##attribute-ontology URI
							# This summary indicates that the GFF3 uses the ontology of attribute names located at the indicated URI or URL.
							line_dic['summary'] = '##attribute-ontology'
							tokens = line_strip.split()[1:]
							if len(tokens) != 1:
								self.add_line_error(line_dic, {
									'message': 'Expecting 1 field, got %d: %s' % (len(tokens) - 1, repr(tokens[1:])),
									'error_type': 'FORMAT', 'location': ''})
							if len(tokens) > 0:
								line_dic['URI'] = tokens[0]
						elif line_strip.startswith('##source-ontology'):
							# ##source-ontology URI
							# This summary indicates that the GFF3 uses the ontology of source names located at the indicated URI or URL.
							line_dic['summary'] = '##source-ontology'
							tokens = line_strip.split()[1:]
							if len(tokens) != 1:
								self.add_line_error(line_dic, {
									'message': 'Expecting 1 field, got %d: %s' % (len(tokens) - 1, repr(tokens[1:])),
									'error_type': 'FORMAT', 'location': ''})
							if len(tokens) > 0:
								line_dic['URI'] = tokens[0]
						elif line_strip.startswith('##species'):
							# ##species NCBI_Taxonomy_URI
							# This summary indicates the species that the annotations apply to.
							line_dic['summary'] = '##species'
							tokens = line_strip.split()[1:]
							if len(tokens) != 1:
								self.add_line_error(line_dic, {
									'message': 'Expecting 1 field, got %d: %s' % (len(tokens) - 1, repr(tokens[1:])),
									'error_type': 'FORMAT', 'location': ''})
							if len(tokens) > 0:
								line_dic['NCBI_Taxonomy_URI'] = tokens[0]
						elif line_strip.startswith('##genome-build'):
							# ##genome-build source buildName
							# The genome assembly build name used for the coordinates given in the file.
							line_dic['summary'] = '##genome-build'
							tokens = line_strip.split()[1:]
							if len(tokens) != 2:
								self.add_line_error(line_dic, {
									'message': 'Expecting 2 fields, got %d: %s' % (len(tokens) - 1, repr(tokens[1:])),
									'error_type': 'FORMAT', 'location': ''})
							if len(tokens) > 0:
								line_dic['source'] = tokens[0]
								try:
									line_dic['buildName'] = tokens[1]
								except IndexError:
									pass
						else:
							self.add_line_error(line_dic, {'message': 'Unknown summary', 'error_type': 'FORMAT',
							                                'location': ''})
							tokens = line_strip.split()
							line_dic['summary'] = tokens[0]
					elif line_strip.startswith('#'):
						line_dic['line_type'] = 'comment'
					version_match = re.match(r'##gff-version\s+(\d+)', line.strip())
					if version_match:
						self._version = version_match.group(1)
						continue
					contig_region_match = re.match(r'##sequence-region\s+(\S+)\s+(\S+)\s+(\S+)', line.strip())
					if contig_region_match:
						self._contigs.append(contig_region_match.group(1))
						self._contig_max_len_dic[contig_region_match.group(1)] = int(contig_region_match.group(3))
						continue
					build_info_match = re.match(r'#!(\S+)\s+(\S+)', line.strip())
					if build_info_match:
						self._build_info_dic[build_info_match.group(1)] = build_info_match.group(2)
						continue
					if re.match(r'^[^#]+\s+.+', line.strip()):
						record_arr = re.split(r'\s+', line.strip())
						if len(record_arr) != 9:
							raise Exception('{}不是合格的gff3文件，包含列数不为9的行：{}'.format(self.path(), line))
						[contig, src, seq_type, start, end, score, strand, phase, attributes] = record_arr
						self._contigs.add(contig)
						self._seq_features.add(seq_type)
						if not contig in self._seq_pos.keys():
							self._seq_pos[contig] = max(start, end)
						if contig in self._seq_pos.keys() and self._seq_pos[contig] < max(start, end):
							self._seq_pos[contig] = max(start, end)
						# 处理第9列
						attribute_array = attributes.strip().split(';')
						item_m = re.match(r'')
		
		self._parse_status = True
	
	def check_contigs(self):
		for contig in self._contigs:
			if not contig.startswith('>') and re.match(r'^[a-zA-Z0-9\.:^*$@!+_?-|]+$', contig):
				pass
			else:
				raise Exception('这个gff文件含有不合法的contig/scaffold/chromosome ID: {}'.format(contig))
	
	'''
	得到每个基因的注释水平
	'''
	
	def get_current_sequence_ontology(self):
		so_file = SequenceOntologyFile(sequence_ontology_file_url)
		so_file.parse()
		pass
	
	def check_features(self):
		for feature in self._seq_features:
			if feature not in self._seq_ontology:
				pass
	
	def get_genbank_assembly_id(self):
		if self._parse_status:
			for item in self._build_info_dic.keys():
				info_macth = re.match(r'NCBI:(\S+)', self._build_info_dic[item])
				if re.match(r'.*genome-build-accession.*', item) and info_macth:
					self._genbank_assembly_id = info_macth.group(1)
			return self._genbank_assembly_id
