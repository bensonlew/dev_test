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
			{"name": "message_table", "type": "infile", "format": "paternity_test.tab"},
			{"name": "origin_data", "type": "infile", "format": "paternity_test.data_dir"},

			{"name": "fastq_path", "type": "infile","format":"sequence.fastq_dir"},  # fastq所在路径(文件夹
			{"name": "cpu_number", "type": "int", "default": 4},  # cpu个数
			{"name": "ref_fasta", "type": "infile","format":"sequence.fasta"},  # 参考序列
			{"name": "targets_bedfile", "type": "infile","format":"denovo_rna.gene_structure.bed"},

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
		self.data_split = self.add_tool("paternity_test.data_split")  # 数据拆分的tool
		self.tools = []
		self.tools_rename = []
		self.tools_rename_analysis = []
		self.tools_dedup =[]
		self.tools_dedup_f = []
		self.rdata = []
		self.set_options(self._sheet.options())
		self.step.add_steps("pt_analysis", "result_info", "retab",
		                    "de_dup1", "de_dup2", "data_split")
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

	def data_split_run(self):
		self.data_split.set_options({
			"message_table": self.option('message_table'),
			"data_dir": self.option('origin_data')
		})
		self.data_split.on('end', self.set_output, 'data_split')
		self.data_split.on('end', self.fastq2tab_run)
		self.data_split.on('start', self.set_step, {'start': self.step.data_split})
		self.data_split.on('end', self.set_step, {'end': self.step.data_split})
		self.data_split.run()

	def fastq2tab_run(self):
		fastq = os.listdir(self.data_split.output_dir + "/wq_data")
		file = []
		father_id = []  # 可能还是需要返回所有样本的名称，而不是只要父亲的, 暂定father_id
		for j in fastq:
			m = re.match('(.*)_R1.fastq.gz', j)
			if m:
				file.append(m.group(1))
				n = re.match('F(.*)', m.group(1).split("-")[1])
				if n:
					father_id.append(m.group(1))
		print(file)
		print("father_id:")
		print(father_id)
		n = 0
		for i in file:
			fastq2tab = self.add_module("paternity_test.fastq2tab")
			self.step.add_steps('fastq2tab{}'.format(n))
			fastq2tab.set_options({
				"sample_id": i,
				"fastq_path": self.data_split.output_dir + "/wq_data",
				"cpu_number": self.option("cpu_number"),
				"ref_fasta": self.option("ref_fasta").prop['path'],
				"targets_bedfile": self.option("targets_bedfile").prop['path'],
			}
			)
			step = getattr(self.step, 'fastq2tab{}'.format(n))
			step.start()
			fastq2tab.on('end', self.finish_update, 'fastq2tab{}'.format(n))
			self.tools.append(fastq2tab)
			n += 1
		for j in range(len(self.tools)):
			self.tools[j].on('end', self.set_output, 'fastq2tab')
		if len(self.tools) > 1:
			self.on_rely(self.tools, self.end)
		else:
			self.tool[0].on('end', self.end)
		for tool in self.tools:
			tool.run()

	def linkdir(self, dirpath, dirname):
		"""
		link一个文件夹下的所有文件到本module的output目录
		:param dirpath: 传入文件夹路径
		:param dirname: 新的文件夹名称
		:return:
		"""
		allfiles = os.listdir(dirpath)
		newdir = os.path.join(self.output_dir, dirname)
		if not os.path.exists(newdir):
			os.mkdir(newdir)
		oldfiles = [os.path.join(dirpath, i) for i in allfiles]
		newfiles = [os.path.join(newdir, i) for i in allfiles]
		for newfile in newfiles:
			if os.path.exists(newfile):
				if os.path.isfile(newfile):
					os.remove(newfile)
				else:
					os.system('rm -r %s' % newfile)
		for i in range(len(allfiles)):
			if os.path.isfile(oldfiles[i]):
				os.link(oldfiles[i], newfiles[i])
			elif os.path.isdir(oldfiles[i]):
				os.system('cp -r %s %s' % (oldfiles[i], newdir))

	def run(self):
		self.data_split_run()
		super(PtProcessWorkflow, self).run()

	def set_output(self, event):
		obj = event["bind_object"]
		if event['data'] == "data_split":
			self.linkdir(obj.output_dir, self.output_dir)
		if event['data'] == "fastq2tab":
			self.linkdir(obj.output_dir + '/bam2tab', self.output_dir)
			api = self.api.tab_file
			temp = os.listdir(self.output_dir)
			api_read_tab = self.api.tab_file  # 二次判断数据库中是否存在tab文件
			for i in temp:
				m = re.search(r'(.*)\.mem.*tab$', i)
				if m:
					tab_path = self.output_dir +'/' + i
					tab_name = m.group(1)
					if not api_read_tab.tab_exist(tab_name):
						api.add_sg_pt_tab_detail(tab_path)

	def end(self):
		super(PtProcessWorkflow, self).end()



