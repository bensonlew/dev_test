# -*- coding: utf-8 -*-
# __author__ :zhouxuan

import shutil
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import os
import re

class DataSplitAgent(Agent):

	"""
		项目：亲子鉴定
		功能：对医学检验所的测序数据进行拆分，区分各个数据(WQ对应亲子鉴定，WS对应产前筛查等)
		author：zhouxuan 2017.02.21
		last modify: 2017.03.16
		version: v1.0

	"""
	def __init__(self, parent):
		super(DataSplitAgent, self).__init__(parent)
		options = [
			{"name": "message_table", "type": "infile", "format": "paternity_test.tab"},
			{"name": "data_dir", "type": "infile", "format": "paternity_test.data_dir"}
			# {"name": "data_dir", "type": "infile", "format": "paternity_test.data_dir"}
		]
		self.add_option(options)
		self.step.add_steps("data_split")
		self.on('start', self.stepstart)
		self.on('end', self.stepfinish)

	def stepstart(self):
		self.step.data_split.start()
		self.step.update()

	def stepfinish(self):
		self.step.data_split.finish()
		self.step.update()

	def check_options(self):
		"""
		重写参数检测函数
		:return:
		"""
		if not self.option('message_table'):
			raise OptionError("必须输入样本信息表")
		if not self.option('data_dir'):
			raise OptionError("必须提供样本序列文件夹")
		return True

	def set_resource(self):  # 后续需要测试确认
		"""
		设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
		:return:
		"""
		self._cpu = 10
		self._memory = '20G'

	def end(self):
		result_dir = self.add_upload_dir(self.output_dir)
		result_dir.add_relpath_rules([
			[".", "", "结果输出目录"]
		])
		result_dir.add_regexp_rules([
			["", "", ""],
		])
		super(DataSplitAgent, self).end()

class DataSplitTool(Tool):
	def __init__(self, config):
		super(DataSplitTool, self).__init__(config)
		self._version = "v1.0"
		self.script_path = "bioinfo/seq/bcl2fastq2-v2.17.1.14/bin/bcl2fastq"
		self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.4.0/lib64')
		self.set_environ(PATH=self.config.SOFTWARE_DIR + '/gcc/5.4.0/bin')

	def run(self):
		"""
		运行
		:return:
		"""
		super(DataSplitTool, self).run()
		self.run_ds()
		self.set_output()
		self.end()

	def run_ds(self):
		"""
		处理输入文件并完成数据拆分
		:return:
		"""
		message_path = self.option('message_table').prop['path']
		new_message_table = os.path.join(self.work_dir, "new_message_table")
		with open(new_message_table, "a") as w:
			w.write("[Data],,,,,,," + "\n" +
			        "Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,Sample_Project,Description" + "\n")
		with open(message_path, "r") as r:
			content = r.readlines()
			for line in content:
				line = line.split("\t")
				with open(new_message_table, "a") as w:
					lines = "Sample_" + line[3] + "," + line[3] + ",,,," + line[8] + "," + line[4] + "," + "\n"
					w.write(lines)
		"""
		当上传为压缩包时用下面的代码获取文件夹路径
		result = os.system('tar -zxf {} -C {}'.format(self.option('data_dir').prop['path'], self.work_dir))
		if result != 0:
			raise OptionError("压缩包解压失败，请重新投递运行！")
		old_data_dir_1 = os.path.join(self.work_dir, self.option('data_dir').prop['path'].split("/")[-1].split(".")[0])
		p = re.search('([0-9]+)$', old_data_dir_1)
		if p:
			old_data_dir = ("_").join(old_data_dir_1.split("_")[0:-1])
		else:
			old_data_dir = old_data_dir_1
		if not os.path.exists(old_data_dir):
			self.set_error("下机数据文件夹路径不正确，请设置正确的路径。")
		"""
		old_data_dir = self.option('data_dir').prop['path']
		cmd = "{} -i {}/Data/Intensities/BaseCalls/ -o {} --sample-sheet {} --use-bases-mask  y76,i6n,y76 " \
		      "--ignore-missing-bcl -R {}/ -r 4 -w 4 -d 2 -p 10 --barcode-mismatches 0".\
			format(self.script_path,old_data_dir,self.work_dir,
		           new_message_table, old_data_dir)
		self.logger.info("start data_split")
		command = self.add_command("ds_cmd", cmd).run()
		self.wait(command)
		if command.return_code == 0:
			self.logger.info("data_split done")
		else:
			self.set_error("data_split error")

	def set_output(self):
		"""
		将数据拆分的结果，直接存放在同一个文件夹下，按照样本分成不同的小文件夹
		:return:
		"""
		undetermined_file = os.listdir(self.work_dir)
		undetermined_sample_file = []
		for file_name in undetermined_file:
			detail_name = file_name.split(".")
			if detail_name[-1] == "gz":
				undetermined_sample_file.append(file_name)
		undetermined_sample_name = []
		for name in undetermined_sample_file:
			sample_ = name.split("_")
			src = os.path.join(self.work_dir, name)
			dir_name = os.path.join(self.output_dir, "Sample_Undetermined_" + sample_[1])
			if sample_[1] not in undetermined_sample_name:
				undetermined_sample_name.append(sample_[1])
				os.mkdir(dir_name)
				#shutil.copy(src, dir_name)
				os.link(src, os.path.join(dir_name, name))
			else:
				#shutil.copy(src, dir_name)
				os.link(src, os.path.join(dir_name, name))
		med_result_dir = os.path.join(self.work_dir, "MED")
		list_name = os.listdir(med_result_dir)
		for name in list_name:
			sample_dir = os.path.join(med_result_dir, name)
			file_name = os.listdir(sample_dir)
			dir_name = os.path.join(self.output_dir, name)
			if not os.path.exists(dir_name):
				os.mkdir(dir_name)
			for name_ in file_name:
				dst = os.path.join(dir_name, name_)
				src = os.path.join(sample_dir, name_)
				os.link(src, dst)

			# dst = os.path.join(self.output_dir, name)
			# shutil.copytree(sample_dir, dst)