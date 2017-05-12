# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'

'''医学检验所-亲子鉴定数据拆分流程'''
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import re
from biocluster.wpm.client import worker_client as WC
import datetime
import json
from mainapp.models.mongo.submit.paternity_test_mongo import PaternityTest as PT
from bson import ObjectId
from biocluster.config import Config

class PtDatasplitWorkflow(Workflow):
	"""
	名称：亲子鉴定数据拆分流程
	作用：完成下机数据的拆分，合并以及分组(wq\ws\undetermined)
	author：zhouxuan
	last_modified: 2017.04.26
	"""
	def __init__(self, wsheet_object):
		self._sheet = wsheet_object
		super(PtDatasplitWorkflow, self).__init__(wsheet_object)
		options = [
			{"name": "message_table", "type": "infile", "format": "paternity_test.tab"},  # 拆分需要的数据表
			# {"name": "data_dir", "type": "infile", "format": "paternity_test.tab"},  # 拆分需要的下机数据压缩文件夹
			{"name": "data_dir", "type": "string"},

			{"name": "family_table", "type": "infile", "format": "paternity_test.tab"},  # 需要存入mongo里面的家系信息表
			{"name": "pt_data_split_id", "type": "string"},
			{"name": "member_id", "type": "string"},
			{"name": "update_info", "type": "string"}
		]
		self.add_option(options)
		self.data_split = self.add_tool("paternity_test.data_split")
		self.merge_fastq = self.add_tool("paternity_test.merge_fastq")
		self.set_options(self._sheet.options())
		# self.update_status_api = self.api.pt_update_status
		self.tools = []
		self.sample_name_wq = []
		self.sample_name_ws = []
		self.sample_name_un = []
		self.data_dir = ''
		self.wq_dir = ''
		self.ws_dir = ''
		self.un_dir = ''
		self.done_wq = ''
		self.done_data_split = ''

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

	def run_data_split(self):
		self.data_split.set_options({
			"message_table": self.option('message_table'),
			"data_dir": self.option('data_dir'),
		})
		self.data_split.on('end', self.run_merge_fastq_wq)
		self.data_split.run()

	def db_customer(self):
		self.logger.info("开始导表(家系表)")
		db_customer = self.api.pt_customer
		db_customer.add_pt_customer(main_id=self.option('pt_data_split_id'),
		                            customer_file=self.option('family_table').prop['path'])
		self.logger.info("导表结束(家系表)")
		self.logger.info("导入样本类型信息")
		db_customer.add_sample_type(self.option('message_table').prop['path'])

	def run_merge_fastq_wq(self):
		self.data_dir = self.data_split.output_dir + "/MED"
		sample_name = os.listdir(self.data_dir)
		for j in sample_name:
			p = re.match('Sample_WQ([0-9].*)-(.*)', j)
			q = re.match('Sample_WS(.*)', j)
			if p:
				self.sample_name_wq.append(j)
			elif q:
				self.sample_name_ws.append(j)
			else:
				self.sample_name_un.append(j)
		n = 0
		self.tools = []
		self.wq_dir = os.path.join(self.output_dir, "wq_dir")
		if not os.path.exists(self.wq_dir):
			os.mkdir(self.wq_dir)
		for i in self.sample_name_wq:
			merge_fastq = self.add_tool("paternity_test.merge_fastq")
			merge_fastq.set_options({
				"sample_dir_name": i,
				"data_dir": self.data_dir,
				"result_dir": self.wq_dir,
			})
			self.tools.append(merge_fastq)
			n += 1
		if len(self.tools) > 1:
			if len(self.sample_name_ws) == 0 and len(self.sample_name_un) != 0:
				self.on_rely(self.tools, self.run_merge_fastq_un)
			elif len(self.sample_name_ws) == 0 and len(self.sample_name_un) == 0:
				self.on_rely(self.tools, self.end)
			else:
				self.on_rely(self.tools, self.run_merge_fastq_ws)
		else:
			if len(self.sample_name_ws) == 0 and len(self.sample_name_un) != 0:
				self.tools[0].on('end', self.run_merge_fastq_un)
			elif len(self.sample_name_ws) == 0 and len(self.sample_name_un) == 0:
				self.tools[0].on('end', self.end)
			else:
				self.tools[0].on('end', self.run_merge_fastq_ws)
		for tool in self.tools:
			tool.run()

	def run_merge_fastq_ws(self):
		self.run_wq_wf()  # 启动亲子鉴定流程和导表工作
		n = 0
		self.tools = []
		self.ws_dir = os.path.join(self.output_dir, "ws_dir")
		if not os.path.exists(self.ws_dir):
			os.mkdir(self.ws_dir)
		for i in self.sample_name_ws:
			merge_fastq = self.add_tool("paternity_test.merge_fastq")
			merge_fastq.set_options({
				"sample_dir_name": i,
				"data_dir": self.data_dir,
				"result_dir": self.ws_dir
			})
			self.tools.append(merge_fastq)
			n += 1
		if len(self.tools) > 1:
			if len(self.sample_name_un) != 0:
				self.on_rely(self.tools, self.run_merge_fastq_un)
			else:
				self.on_rely(self.tools, self.end)
		else:
			if len(self.sample_name_un) != 0:
				self.tools[0].on('end', self.run_merge_fastq_un)
			else:
				self.tools[0].on('end', self.end)
		for tool in self.tools:
			tool.run()

	def run_wq_wf(self):  # 亲子鉴定流程
		self.logger.info("给pt_batch传送数据路径")
		mongo_data = [
			('batch_id', ObjectId(self.option('pt_data_split_id'))),
			("type", "pt"),
			("created_ts", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
			("status", "start"),
			("member_id",self.option('member_id'))
		]
		main_table_id = PT().insert_main_table('sg_analysis_status', mongo_data)
		update_info = {str(main_table_id): 'sg_analysis_status'}
		update_info = json.dumps(update_info)
		data = {
			'stage_id': 0,
			'UPDATE_STATUS_API': self._update_status_api(),
			"id": 'pt_batch' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
			"type": "workflow",
			"name": "paternity_test.patch_dc_backup",
			"instant": False,
			"IMPORT_REPORT_DATA": True,
			"IMPORT_REPORT_AFTER_END": False,
			"options": {
				"member_id": self.option('member_id'),
				"fastq_path": self.wq_dir,
				"cpu_number": 8,
				"ref_fasta": Config().SOFTWARE_DIR + "/database/human/hg38.chromosomal_assembly/ref.fa",
				"targets_bedfile": Config().SOFTWARE_DIR + "/database/human/pt_ref/snp.chr.sort.3.bed",
				"ref_point": Config().SOFTWARE_DIR + "/database/human/pt_ref/targets.bed.rda",
				"err_min": 2,
				"batch_id": self.option('pt_data_split_id'),
				"dedup_num": 2,
				"update_info": update_info
			}
		}
		WC().add_task(data)
		self.done_wq = "true"
		self.logger.info("亲子鉴定数据拆分结束，pt_batch流程开始")

	def _update_status_api(self):
		name = self.option('data_dir').split(":")[0]
		if name == "sanger" or name == "i-sanger":
			return 'pt.med_report_update'
		else:
			return 'pt.med_report_tupdate'

	def run_merge_fastq_un(self):
		if self.done_wq != "true":
			self.run_wq_wf()
		n = 0
		self.tools = []
		self.un_dir = os.path.join(self.output_dir, "undetermined_dir")
		if not os.path.exists(self.un_dir):
			os.mkdir(self.un_dir)
		for i in self.sample_name_un:
			merge_fastq = self.add_tool("paternity_test.merge_fastq")
			merge_fastq.set_options({
				"sample_dir_name": i,
				"data_dir": self.data_dir,
				"result_dir": self.un_dir
			})
			self.tools.append(merge_fastq)
			n += 1
		if len(self.tools) > 1:
			self.on_rely(self.tools, self.end)
		else:
			self.tools[0].on('end', self.end)
		for tool in self.tools:
			tool.run()

	def run(self):
		"""
		判断这组数据是不是已经跑过拆分了，如果数据库中已有，说明已经有wq和ws以及undetermined的路径了
	    判断路径是否存在，如果存在给self.wq_dir等赋值，如果不存在，直接重跑
		:return:
		"""
		self.db_customer()  # 家系表导表，不管是否做过拆分导表都进行一下
		db_customer = self.api.pt_customer
		dir_list = db_customer.get_wq_dir(self.option('data_dir').split(":")[1])
		self.logger.info(dir_list)
		if len(dir_list) == 3 and (os.path.exists(dir_list[0]) or os.path.exists(dir_list[1])):
			self.wq_dir = dir_list[0]
			self.ws_dir = dir_list[1]
			self.un_dir = dir_list[2]
			self.start_listener()
			self.end()
		else:
			self.done_data_split = "true"
			self.run_data_split()
			super(PtDatasplitWorkflow, self).run()

	def set_output(self, event):  # 暂时无用
		obj = event["bind_object"]
		if event['data'] == "data_split":
			self.linkdir(obj.output_dir, self.output_dir + "/data_split")
		if event['data'] == "merge_fastq":
			wq_dir = os.path.join(self.output_dir, "wq_dir")
			self.wq_dir = wq_dir
			ws_dir = os.path.join(self.output_dir, "ws_dir")
			undetermined_dir = os.path.join(self.output_dir, "undetermined_dir")
			if not os.path.exists(wq_dir):
				os.mkdir(wq_dir)
			if not os.path.exists(ws_dir):
				os.mkdir(ws_dir)
			if not os.path.exists(undetermined_dir):
				os.mkdir(undetermined_dir)
			file_name = os.listdir(obj.output_dir)
			m = re.match('WQ([0-9].*)-(.*)', file_name[0])  # wq
			n = re.match('WS(.*)', file_name[0])  # ws
			if m:
				self.linkdir(obj.output_dir, wq_dir)
			else:
				if n:
					self.linkdir(obj.output_dir, ws_dir)
				else:
					self.linkdir(obj.output_dir, undetermined_dir)

	def end(self):
		self.logger.info("医学流程数据拆分结束")
		if self.done_data_split == "true":
			self.logger.info("开始导入拆分结果路径")
			db_customer = self.api.pt_customer
			db_customer.add_data_dir(self.option('data_dir').split(":")[1], self.wq_dir, self.ws_dir, self.un_dir)
		if self.done_wq != "true":
			self.run_wq_wf()
		super(PtDatasplitWorkflow, self).end()

	def linkdir(self, dirpath, dirname):  # 暂时无用
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
				file_name = os.listdir(oldfiles[i])
				os.mkdir(newfiles[i])
				for file_name_ in file_name:
					os.link(os.path.join(oldfiles[i], file_name_), os.path.join(newfiles[i], file_name_))
