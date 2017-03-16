# -*- coding: utf-8 -*-
# __author__ :zhouxuan

import shutil
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import os
import re

class MergeFastqAgent(Agent):
	"""
	医学流程数据拆分部分fastq.gz合并
	author：xuan.zhou
	last_modify: 2017.03.08
	"""

	def __init__(self, parent):
		super(MergeFastqAgent, self).__init__(parent)
		options = [
			{"name": "sample_dir_name", "type": "string"},
			{"name": "data_dir", "type": "infile", "format": "paternity_test.data_dir"}
		]
		self.add_option(options)
		self.step.add_steps("merge_fastq")
		self.on('start', self.stepstart)
		self.on('end', self.stepfinish)
		self.r1_path = ''
		self.r2_path = ''



	def stepstart(self):
		self.step.merge_fastq.start()
		self.step.update()

	def stepfinish(self):
		self.step.merge_fastq.finish()
		self.step.update()

	def check_options(self):
		"""
		重写参数检测函数
		:return:
		"""
		if not self.option('sample_dir_name'):
			raise OptionError("必须输入指定样本文件夹名称")
		if not self.option('data_dir'):
			raise OptionError("必须提供样本序列文件夹")
		return True

	def set_resource(self):  # 后续需要测试确认
		"""
		设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
		:return:
		"""
		self._cpu = 5
		self._memory = '10G'

	def end(self):
		result_dir = self.add_upload_dir(self.output_dir)
		result_dir.add_relpath_rules([
			[".", "", "结果输出目录"]
		])
		result_dir.add_regexp_rules([
			["", "", ""],
		])
		super(MergeFastqAgent, self).end()

class MergeFastqTool(Tool):
	def __init__(self, config):
		super(MergeFastqTool, self).__init__(config)
		self._version = "v1.0"
			# self.script_path = "bioinfo/seq/bcl2fastq2-v2.17.1.14/bin/bcl2fastq"
			# self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64')

	def run(self):
		"""
		运行
		:return:
		"""
		super(MergeFastqTool, self).run()
		self.run_mf()
		self.set_output()
		self.end()

	def run_mf(self):
		"""
		fastq.gz合并(单个样本)
		:return:
		"""
		file_path = os.path.join(self.option("data_dir").prop['path'], self.option("sample_dir_name"))
		print file_path
		file_name = os.listdir(file_path)
		for name in file_name:
			lane_path = os.path.join(file_path, name)
			os.system('gunzip {}'.format(lane_path))
		new_file_list = os.listdir(file_path)
		r1_list = []
		r2_list = []
		for p in new_file_list:
			m = re.match('(.*)_R1_([0-9].*).fastq', p)
			if m:
				r1_list.append(p)
			else:
				r2_list.append(p)
		r1_list.sort()
		r2_list.sort()
		sample_name_ = self.option("sample_dir_name").split("_")
		new_name = ("_").join(sample_name_[1:])
		self.r1_path = os.path.join(file_path, new_name + "_R1.fastq")
		print(self.r1_path)
		self.r2_path = os.path.join(file_path, new_name + "_R2.fastq")
		r1_path = []
		for q in r1_list:
			new_file_path = os.path.join(file_path, q)
			r1_path.append(new_file_path)
		os.system('cat {} {} {} {} >> {}'.format(r1_path[0], r1_path[1], r1_path[2], r1_path[3], self.r1_path))
		r2_path = []
		for l in r2_list:
			new_file_path_2 = os.path.join(file_path, l)
			r2_path.append(new_file_path_2)
		os.system('cat {} {} {} {} >> {}'.format(r2_path[0], r2_path[1], r2_path[2], r2_path[3], self.r2_path))
		os.system('gzip {}'.format(self.r1_path))
		os.system('gzip {}'.format(self.r2_path))

	def set_output(self):
		"""
		把合并成功的所有双端fastq序列放在结果文件夹中
		:return:
		"""
		gz_r1_file_path = self.r1_path + ".gz"
		gz_r2_file_path = self.r2_path + ".gz"
		if os.path.exists(gz_r1_file_path):
			shutil.copy(gz_r1_file_path, self.output_dir)
		else:
			self.set_error("no R1_fastq.gz file")
		if os.path.exists(gz_r2_file_path):
			shutil.copy(gz_r2_file_path, self.output_dir)
		else:
			self.set_error("no R2_fastq.gz file")