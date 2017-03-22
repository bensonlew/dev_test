# -*- coding: utf-8 -*-
# __author__ = fiona
# time: 2017/3/10 18:07

import re, os, Bio, argparse, sys, fileinput, urllib2
import Bio
import subprocess
from bs4 import BeautifulSoup
from file import File
from gff3_file import Gff3File


class SequenceOntologyFile(File):
	def __init__(self, site):
		self._path = os.path.join(os.getcwd(), 'temp_sequence_ontology.obo')
		self._download(site)
		self._items_info = dict()
		self._items = set()
		self._update_date = None
	
	def _download(self, url):
		subprocess.call('wget -c -O {} {}'.format(self._path, url), shell=True)
	
	def __get_ele_indexes(self, lst, pattern):
		indexes = []
		for e in lst:
			if re.match(pattern, e):
				indexes.append(lst.index(e))
		return indexes
	
	def __get_split_lst_by_given_indexes(self,lst,indexes):
		sub_lst = lst[indexes[0]+1:indexes[len(indexes)-1]]
		couple_lst = []
		for i in range(len(indexes)):
			if i>0:
				sub_lst = lst[indexes[i-1]:indexes[i]]
				couple_lst.append(sub_lst)
			
	
	def parse(self):
		so_soup = BeautifulSoup(open(self._path).read())
		records = so_soup.find_all('td', text=re.compile(r'^(?:\[Term\]|name:|id:)'), attrs={'class': 'blob-code blob-code-inner js-file-line'})
		field_pattern = re.compile(r'<td.+>\[Term\]</td>')
		cut_indexes = self.__get_ele_indexes(records, field_pattern)
		self.__get_split_lst_by_given_indexes(records, cut_indexes)
