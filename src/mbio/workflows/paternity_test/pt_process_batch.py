# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'

'''医学检验所-无创产前亲子鉴定流程'''
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import re
import json
import shutil

class PtProcessWorkflow(Workflow):
	def __init__(self, wsheet_object):
		'''
		:param wsheet_object:
		'''
		self._sheet = wsheet_object
		super(PtProcessWorkflow, self).__init__(wsheet_object)
		options = [
			{"name": "fastq_path", "type": "infile","format":"sequence.fastq_dir"},  # fastq所在路径(文件夹
			{"name": "cpu_number", "type": "int", "default": 4},  # cpu个数
			{"name": "ref_fasta", "type": "infile","format":"sequence.fasta"},  # 参考序列
			{"name": "targets_bedfile", "type": "infile","format":"denovo_rna.gene_structure.bed"},

			{"name": "dad_id", "type": "string"},  # 输入F/M/S的样本ID
			{"name": "mom_id", "type": "string"},
			{"name": "preg_id", "type": "string"},
			{"name": "err_min", "type": "int", "default": 2},  # 允许错配数
			{"name": "ref_point", "type": "infile","format":"sequence.rda"},  # 参考位点
			{"name": "dedup_num", "type": "int", "default": 50},  # 查重样本数

			{"name": "second_sample_f", "type": "bool", "default": False},  # 是否重送样或二次上机
			{"name": "second_sample_m", "type": "bool","default": False},  # 是否重送样或二次上机
			{"name": "second_sample_s", "type": "bool", "default": False},  # 是否重送样或二次上机

		]
		self.add_option(options)
		self.pt_analysis = self.add_module("paternity_test.pt_analysis")
		self.result_info = self.add_tool("paternity_test.result_info")
		self.tools = []
		self.tools_rename = []
		self.tools_rename_analysis = []
		self.tools_dedup =[]
		self.tools_dedup_f = []
		self.rdata = []
		self.set_options(self._sheet.options())
		self.step.add_steps("pt_analysis", "result_info", "retab",
		                    "de_dup1", "de_dup2")
		self.update_status_api = self.api.pt_update_status

	def check_options(self):
		'''
		检查参数设置
		'''

		if not self.option("dad_id"):
			raise OptionError("必须输入父本编号")
		if not self.option("mom_id"):
			raise OptionError("必须输入母本编号")
		if not self.option("preg_id"):
			raise OptionError("必须输入胎儿编号")
		if not self.option("ref_fasta"):
			raise OptionError("必须输入参考基因组的fastq文件")
		if not self.option('fastq_path').is_set:
			raise OptionError('必须提供fastq文件所在的路径')
		if not self.option('targets_bedfile'):
			raise OptionError('必须提供target_bedfile文件')
		return True

	def set_step(self,event):
		if 'start' in event['data'].keys():
			event['data']['start'].start()
		if 'end' in event['data'].keys():
			event['data']['end'].finish()
		self.step.update()

	def finish_update(self, event):
		step = getattr(self.step, event['data'])
		step.finish()
		self.step.update()