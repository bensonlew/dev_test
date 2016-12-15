# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
#last modified:20161213

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
			{"name": "fastq_path", "type": "string"},  # fastq所在路径(文件夹
			{"name": "cpu_number", "type": "int", "default": 4},  # cpu个数
			{"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"},  # 参考序列
			{"name": "targets_bedfile", "type": "string"},

			{"name": "dad_id", "type": "string"},  # 输入F/M/S的样本ID
			{"name": "mom_id", "type": "string"},
			{"name": "preg_id", "type": "string"},
			{"name": "err_min", "type": "int", "default": 2},  # 允许错配数
			{"name": "ref_point", "type": "string"},  # 参考位点
			{"name": "dedup_num", "type": "int", "default": 50},  # 查重样本数

		]
		self.add_option(options)
		self.pt_analysis = self.add_module("paternity_test.pt_analysis")
		self.result_info = self.add_tool("paternity_test.result_info")
		self.tools = []
		self.tools_dedup =[]
		self.set_options(self._sheet.options())
		self.step.add_steps("pt_analysis", "result_info", "retab",
		                    "de_dup1", "de_dup2")

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
		if not self.option("ref_fasta").is_set:
			raise OptionError("必须输入参考基因组的fastq文件")
		if not self.option('fastq_path'):
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


	def fastq2tab_run(self):
		api_read_tab = self.api.tab_file
		fastq = os.listdir(self.option('fastq_path'))
		file = []
		for j in fastq:
			m = re.match('(.*)_R1.fastq.gz', j)
			if m:
				file.append(m.group(1))
		n = 0
		for i in file:
			if not api_read_tab.tab_exist(i):
				fastq2tab = self.add_module("paternity_test.fastq2tab")
				self.step.add_steps('fastq2tab{}'.format(n))
				fastq2tab.set_options({
					"sample_id": i,
					"fastq_path": self.option("fastq_path"),
					"cpu_number": self.option("cpu_number"),
					"ref_fasta": self.option("ref_fasta"),
					"targets_bedfile": self.option("targets_bedfile"),
				}
				)
				step = getattr(self.step, 'fastq2tab{}'.format(n))
				step.start()
				fastq2tab.on('end', self.finish_update, 'fastq2tab{}'.format(n))
				self.tools.append(fastq2tab)
				n += 1
			else:
				self.logger.info('{}样本已存在于数据库'.format(i))
		for j in range(len(self.tools)):
			self.tools[j].on('end', self.set_output, 'fastq2tab')
		if len(self.tools) > 1:
			self.on_rely(self.tools, self.pt_analysis_run)
		elif len(self.tools) == 1:
			self.tools[0].on('end', self.pt_analysis_run)
		for tool in self.tools:
			# tool.on('end', self.pt_analysis_run)
			tool.run()


	def pt_analysis_run(self):
		api_read_tab = self.api.tab_file
		self.pt_analysis.set_options({
			"dad_tab": api_read_tab.export_tab_file(self.option('dad_id'), self.output_dir),  # 数据库的tab文件
			"mom_tab": api_read_tab.export_tab_file(self.option('mom_id'), self.output_dir),
			"preg_tab": api_read_tab.export_tab_file(self.option('preg_id'), self.output_dir),
			"ref_point": self.option("ref_point"),
			"err_min": self.option("err_min")
		})
		self.pt_analysis.on('end', self.set_output, 'pt_analysis')
		self.pt_analysis.on('start', self.set_step, {'start': self.step.pt_analysis})
		self.pt_analysis.on('end', self.set_step, {'end': self.step.pt_analysis})
		self.pt_analysis.run()

	def result_info_run(self):
		self.result_info.set_options({
			"tab_merged":  self.output_dir+'/family_joined_tab.Rdata'
		})
		self.result_info.on('end', self.set_output, 'result_info')
		self.result_info.on('start', self.set_step, {'start': self.step.result_info})
		self.result_info.on('end', self.set_step, {'end': self.step.result_info})
		self.result_info.run()

	def dedup1_run(self):
		api_read_tab = self.api.tab_file
		n = 0
		temp = re.match('WQ([1-9].*)-F.*', self.option('dad_id'))
		num = int(temp.group(1))
		num_list = range(num, num+self.option('dedup_num'))
		name_list = []
		for m in num_list:
			x = api_read_tab.dedup_sample(m)
			for k in range(len(x)):
				name_list.append(x[k])
		for i in name_list:
			pt_analysis_dedup1 = self.add_module("paternity_test.pt_analysis")
			self.step.add_steps('dedup1_{}'.format(n))
			pt_analysis_dedup1.set_options({
					"dad_tab": api_read_tab.export_tab_file(i, self.output_dir),  # 数据库的tab文件
					"mom_tab": api_read_tab.export_tab_file(self.option('mom_id'), self.output_dir),
					"preg_tab": api_read_tab.export_tab_file(self.option('preg_id'), self.output_dir),
					"ref_point": self.option("ref_point"),
					"err_min": self.option("err_min")
			}
			)
			step = getattr(self.step, 'dedup1_{}'.format(n))
			step.start()
			pt_analysis_dedup1.on('end', self.finish_update, 'dedup1_{}'.format(n))
			self.tools_dedup.append(pt_analysis_dedup1)
			n += 1
		for j in range(len(self.tools_dedup)):
			self.tools_dedup[j].on('end', self.set_output, 'dedup1')
		self.on_rely(self.tools_dedup, self.end)
		for tool in self.tools_dedup:
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

	def set_output(self, event):
		obj = event["bind_object"]
		if event['data'] == "fastq2tab":
			self.linkdir(obj.output_dir + '/bam2tab', self.output_dir)
			api = self.api.tab_file
			temp = os.listdir(obj.output_dir + '/bam2tab')
			api_read_tab = self.api.tab_file  # 二次判断数据库中是否存在tab文件
			for i in temp:
				m = re.search(r'(.*)\.mem.*tab$', i)
				if m:
					tab_path = self.output_dir +'/' + i
					tab_name = m.group(1)
					if not api_read_tab.tab_exist(tab_name):
						api.add_sg_pt_tab_detail(tab_path)

		if event['data'] == "pt_analysis":
			self.linkdir(obj.output_dir +'/family_analysis', self.output_dir)
			self.linkdir(obj.output_dir + '/family_merge', self.output_dir)
			api_pt = self.api.sg_paternity_test
			results = os.listdir(obj.output_dir +'/family_analysis')
			for f in results:
				if re.search(r'.*family_analysis\.txt$', f):
					api_pt.add_analysis_tab(self.output_dir+'/'+f)

			result_1 = os.listdir(obj.output_dir+'/family_merge')
			for f in result_1:
				if re.search(r'.*family_joined_tab\.txt$', f):
					api_pt.add_sg_pt_family_detail(self.output_dir+'/'+f)

		if event['data'] == "result_info":
			self.linkdir(obj.output_dir, self.output_dir)

		if event['data'] == "dedup1":
			self.linkdir(obj.output_dir + '/family_analysis', self.output_dir)
			# self.linkdir(obj.output_dir + '/family_merge', self.output_dir)
			api_pt = self.api.sg_paternity_test
			results = os.listdir(obj.output_dir + '/family_analysis')
			for f in results:
				if re.search(r'.*family_analysis\.txt$', f):
					api_pt.add_analysis_tab(self.output_dir+'/'+f)

	def run(self):
		self.fastq2tab_run()
		self.pt_analysis.on('end', self.result_info_run)
		self.result_info.on('end', self.dedup1_run)
		if not self.tools:
			self.pt_analysis_run()
		super(PtProcessWorkflow, self).run()


	def end(self):
		super(PtProcessWorkflow,self).end()
