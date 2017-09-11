# -*- coding: utf-8 -*-
# __author__ :zhouxuan

from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import os
import subprocess
import math

class HmmscanAgent(Agent):
	"""
	为cazy注释写的比对软件 hummscan
	version 1.0
    author: zhouxuan
    last_modify: 20170524
	"""

	def __init__(self, parent):
		super(HmmscanAgent, self).__init__(parent)
		options = [
			{"name": "faa_file", "type": "infile", "format": "sequence.fasta"},
			{"name": "hmmscan_out_dm", "type": "outfile", 'format': "meta_genomic.hmmscan_table"}  # 结果文件的设置
		]
		self.add_option(options)
		self.step.add_steps("hmmscan")
		self.on('start', self.stepstart)
		self.on('end', self.stepfinish)

	def stepstart(self):
		self.step.hmmscan.start()
		self.step.update()

	def stepfinish(self):
		self.step.hmmscan.finish()
		self.step.update()

	def check_options(self):
		"""
		重写参数检测函数
		:return:
		"""
		if not self.option('faa_file'):
			raise OptionError("必须输入fastq文件")
		return True

	def set_resource(self):  # 后续需要测试确认
		"""
		设置所需资源，需在之类中重写此方法 self._cpu ,self._memory
		"""
		self._cpu = 10
		self._memory = '10G'

	def end(self):
		result_dir = self.add_upload_dir(self.output_dir)
		result_dir.add_relpath_rules([
			[".", "", "结果输出目录"]
		])
		result_dir.add_regexp_rules([
			["", "", ""],
		])
		super(HmmscanAgent, self).end()

class HmmscanTool(Tool):
	def __init__(self, config):
		super(HmmscanTool, self).__init__(config)
		self._version = "v1.0"
		self.soft_path = "bioinfo/align/hmmer-3.1b2-linux-intel-x86_64/binaries/hmmscan"
		self.data_path = self.config.SOFTWARE_DIR + "/database/CAZyDB/dbCAN-fam-HMMs.txt.v5"

	def run(self):
		"""
		运行
		:return:
		"""
		super(HmmscanTool, self).run()
		self.run_hmm()
		self.set_output()
		self.end()

	def run_hmm(self):
		hmm_out_dm = os.path.join(self.output_dir, "dbCAN.hmmscan.out.dm")
		hmm_out = os.path.join(self.output_dir, "dbCAN.hmmscan.out")
		cmd = "{} --domtblout {} -o {} --cpu 6 {} {}"\
			.format(self.soft_path, hmm_out_dm, hmm_out, self.data_path, self.option("faa_file").prop['path'])
		self.logger.info("start run_hmm")
		command = self.add_command("hmmscan", cmd).run()
		self.wait(command)
		if command.return_code == 0:
			self.logger.info("run_hmm done")
		else:
			command.rerun()
			self.wait(command)
			if command.return_code == 0:
				self.logger.info("run_hmm done")
			else:
				self.set_error("run_hmm error")
				raise Exception("run_hmm error")

	def set_output(self):
		if len(os.listdir(self.output_dir)) == 2:
			self.logger.info("hmm比对的结果文件正确生成")
			number = os.path.basename(self.option('faa_file').prop['path']).split('_')[-1]
			os.remove(os.path.join(self.output_dir, "dbCAN.hmmscan.out"))
			os.link(os.path.join(self.output_dir, "dbCAN.hmmscan.out.dm"),
			        os.path.join(self.output_dir, "dbCAN.hmmscan.out.dm_{}".format(number)))
			os.remove(os.path.join(self.output_dir, "dbCAN.hmmscan.out.dm"))
			self.logger.info('dbCAN.hmmscan.out原始比对结果删除')
			self.option("hmmscan_out_dm").set_path(self.output_dir + '/dbCAN.hmmscan.out.dm_{}'.format(number))
			# self.option('hmmscan_out_dm', self.output_dir + '/dbCAN.hmmscan.out.dm_{}'.format(number))
		else:
			raise Exception("hmm比对结果出错")