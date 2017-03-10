# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'

'''医学检验所-亲子鉴定数据拆分流程'''
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import re
import json
import shutil

class PtDatasplitWorkflow(Workflow):
	"""
	名称：亲子鉴定数据拆分流程
	作用：完成下机数据的拆分，合并以及分组(wq\ws\undetermined)
	author：zhouxuan
	last_modified: 2017.03.10
	"""
	def __init__(self, wsheet_object):
		self._sheet = wsheet_object
		super(PtDatasplitWorkflow, self).__init__(wsheet_object)
		options = [
			{"name": "message_table", "type": "infile", "format": "paternity_test.tab"},  # 拆分需要的数据表
			{"name": "data_dir", "type": "infile", "format": "paternity_test.data_dir"},  # 拆分需要的下机数据

			{"name": "family_table", "type": "infile", "format": "paternity_test.tab"},  # 需要存入mongo里面的家系信息表
			{"name": "pt_data_split_id", "type": "string"},
			{"name": "update_info", "type": "string"}
		]
		self.add_option(options)
		self.data_split = self.add_tool("paternity_test.data_split")
		self.merge_fastq = self.add_tool("paternity_test.merge_fastq")
		self.set_options(self._sheet.options())
		self.step.add_steps("data_split", "merge_fastq")
		self.update_status_api = self.api.pt_update_status
		self.tools = []

	def check_options(self):
		'''
		检查参数设置
		'''

		if not self.option("message_table"):
			raise OptionError("缺少拆分需要的数据表")
		if not self.option("data_dir"):
			raise OptionError("缺少拆分需要的下机数据")
		if not self.option("family_table"):
			raise OptionError("缺少家系信息表")
		return True

	def set_step(self, event):
		if 'start' in event['data'].keys():
			event['data']['start'].start()
		if 'end' in event['data'].keys():
			event['data']['end'].finish()
		self.step.update()

	def finish_update(self, event):
		step = getattr(self.step, event['data'])
		step.finish()
		self.step.update()

	def run_data_split(self):
		self.data_split.set_options({
			"message_table": self.option('message_table'),
			"data_dir": self.option('data_dir')
		})
		self.data_split.on('end', self.set_output, 'data_split')
		self.data_split.on('end', self.run_merge_fastq)
		self.data_split.on('start', self.set_step, {'start': self.step.data_split})
		self.data_split.on('end', self.set_step, {'end': self.step.data_split})
		self.data_split.run()

	def run_merge_fastq(self):
		data_dir = self.data_split.output_dir
		sample_name = os.listdir(data_dir)
		n = 0
		for i in sample_name:
			merge_fastq = self.add_tool("paternity_test.merge_fastq")
			self.step.add_steps('merge_fastq{}'.format(n))
			merge_fastq.set_options({
				"sample_dir_name": i,
				"data_dir": data_dir
			})
			step = getattr(self.step, 'merge_fastq{}'.format(n))
			step.start()
			merge_fastq.on('end', self.finish_update, 'merge_fastq{}'.format(n))
			self.tools.append(merge_fastq)
			n += 1
		for j in range(len(self.tools)):
			self.tools[j].on('end', self.set_output, 'merge_fastq')
		if len(self.tools) > 1:
			self.on_rely(self.tools, self.end)
		else:
			self.tool[0].on('end', self.end)
		for tool in self.tools:
			tool.run()

	def run(self):
		self.run_data_split()
		super(PtDatasplitWorkflow, self).run()

	def set_output(self, event):
		obj = event["bind_object"]
		if event['data'] == "data_split":
			self.linkdir(obj.output_dir, self.output_dir)
		if event['data'] == "merge_fastq":
			os.mkdir(os.path.join(self.output_dir, "wq_dir"))
			os.mkdir(os.path.join(self.output_dir, "ws_dir"))
			os.mkdir(os.path.join(self.output_dir, "undetermined_dir"))
			file_name = os.listdir(obj.output_dir)
			m = re.match('WQ(.*)', file_name[0])  # wq
			n = re.match('WS-(.*)', file_name[0])  # ws
			l = re.search('Undetermined', file_name[0])  # undetermined
			if m:
				self.linkdir(obj.output_dir, os.path.join(self.output_dir, "wq_dir"))
			if n:
				self.linkdir(obj.output_dir, os.path.join(self.output_dir, "ws_dir"))
			if l:
				self.linkdir(obj.output_dir, os.path.join(self.output_dir, "undetermined_dir"))
			# api = self.api.tab_file
			# temp = os.listdir(self.output_dir)
			# api_read_tab = self.api.tab_file  # 二次判断数据库中是否存在tab文件
			# for i in temp:
			# 	m = re.search(r'(.*)\.mem.*tab$', i)
			# 	if m:
			# 		tab_path = self.output_dir + '/' + i
			# 		tab_name = m.group(1)
			# 		if not api_read_tab.tab_exist(tab_name):
			# 			api.add_sg_pt_tab_detail(tab_path)

	def end(self):
		super(PtDatasplitWorkflow, self).end()

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

