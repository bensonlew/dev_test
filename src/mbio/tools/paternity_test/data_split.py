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
		author：zhouxuan
		last modify: 2017.02.21
					 2017.02.28
		version: v1.0

	"""
	def __init__(self, parent):
		super(DataSplitAgent, self).__init__(parent)
		options = [
			{"name": "message_table", "type": "infile", "format": "paternity_test.tab"},
			{"name": "data_dir", "type": "infile", "format": "paternity_test.data_dir"}
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
		self._memory = '100G'

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
		# self.script_path = Config().SOFTWARE_DIR + '/bioinfo/seq/bcl2fastq2-v2.17.1.14/bin/bcl2fastq'
		self.script_path = "bioinfo/seq/bcl2fastq2-v2.17.1.14/bin/bcl2fastq"
		self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64')

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
		cmd = "{} -i {}Data/Intensities/BaseCalls/ -o {} --sample-sheet {} --use-bases-mask  y76,i6n,y76 " \
		      "--ignore-missing-bcl -R {} -r 4 -w 4 -d 2 -p 10 --barcode-mismatches 0".\
			format(self.script_path,self.option('data_dir').prop['path'],self.work_dir,
		           new_message_table, self.option('data_dir').prop['path'])
		self.logger.info("start data_split")
		command = self.add_command("ds_cmd", cmd).run()
		self.wait(command)
		if command.return_code == 0:
			self.logger.info("data_split done")
		else:
			self.set_error("data_split error")

	def set_output(self):
		"""
		把拆分所得的结果文件放置在tool的output文件夹下按照(WQ对应亲子鉴定，WS对应产前筛查等，undetermined对应暂时不确定的fastq文件)
		:return: logger message
		"""
		med_result_dir = os.path.join(self.work_dir, "MED")
		if not os.path.exists(med_result_dir):
			self.logger.info("no result_dir of data_split")
			self.set_error("no result_dir of data_split")
		wq_dir = os.path.join(self.output_dir, "wq_data")
		os.mkdir(wq_dir)
		ws_dir = os.path.join(self.output_dir, "ws_data")
		os.mkdir(ws_dir)
		undetermined_dir = os.path.join(self.output_dir, "undetermined_data")
		os.mkdir(undetermined_dir)
		undetermined_name = os.listdir(self.work_dir)
		unknown_sample_name = []
		unknown_sample = []
		for i in undetermined_name:
			detail_name = i.split(".")
			if detail_name[-1] == "gz":
				unknown_sample.append(i)
				name = i.split("_")
				if name[1] not in unknown_sample_name:
					unknown_sample_name.append(name[1])
		for name in unknown_sample_name:
			for file_name in unknown_sample:
				file_name_ = file_name.split("_")
				if file_name_[1] == name:
					if file_name_[3] == "R1":
						os.system('cat {} >> {}'.format(os.path.join(self.work_dir, file_name), os.path.join(undetermined_dir, "Undetermined_" + name + "_R1.fastq.gz")))
					else:
						os.system('cat {} >> {}'.format(os.path.join(self.work_dir, file_name), os.path.join(undetermined_dir, "Undetermined_" + name + "_R2.fastq.gz")))
		sample_dir_name = os.listdir(med_result_dir)
		for dir_name in sample_dir_name:
			med_sample = dir_name.split("_")
			med_sample_name = med_sample[1]
			part_of_name = med_sample_name.split("-")
			if part_of_name[0] == "WS":
				part_seq_name = os.listdir(os.path.join(med_result_dir, dir_name))
				for name in part_seq_name:
					seq_name = name.split("_")
					if seq_name[3] == "R1":
						os.system('cat {} >> {}'.format(os.path.join(med_result_dir, dir_name, name), os.path.join(ws_dir, med_sample_name + "_R1.fastq.gz")))
					else:
						os.system('cat {} >> {}'.format(os.path.join(med_result_dir, dir_name, name), os.path.join(ws_dir, med_sample_name + "_R2.fastq.gz")))
			else:
				m = re.match('WQ([1-9].*)', part_of_name[0])
				if m:
					part_seq_name = os.listdir(os.path.join(med_result_dir, dir_name))
					for name in part_seq_name:
						seq_name = name.split("_")
						if seq_name[3] == "R1":
							os.system('cat {} >> {}'.format(os.path.join(med_result_dir, dir_name, name), os.path.join(wq_dir, med_sample_name + "_R1.fastq.gz")))
						else:
							os.system('cat {} >> {}'.format(os.path.join(med_result_dir, dir_name, name), os.path.join(wq_dir, med_sample_name + "_R2.fastq.gz")))
				else:
					part_seq_name = os.listdir(os.path.join(med_result_dir, dir_name))
					for name in part_seq_name:
						seq_name = name.split("_")
						if seq_name[3] == "R1":
							os.system('cat {} >> {}'.format(os.path.join(med_result_dir, dir_name, name), os.path.join(undetermined_dir, med_sample_name + "_R1.fastq.gz")))
						else:
							os.system('cat {} >> {}'.format(os.path.join(med_result_dir, dir_name, name), os.path.join(undetermined_dir, med_sample_name + "_R2.fastq.gz")))

		# if os.path.exists(result_dir):
		# 	new_data_dir = os.path.join(self.output_dir, "med_data")
		# 	try:
		# 		shutil.copytree(result_dir, new_data_dir)
		# 	except Exception as e:
		# 		self.logger.info("set output failed{}".format(e))
		# 		self.set_error("set output failed{}".format(e))
		# else:
		# 	self.logger.info("no result_dir")
