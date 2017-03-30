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

class PtDatasplitWorkflow(Workflow):
	"""
	名称：亲子鉴定数据拆分流程
	作用：完成下机数据的拆分，合并以及分组(wq\ws\undetermined)
	author：zhouxuan
	last_modified: 2017.03.23
	"""
	def __init__(self, wsheet_object):
		self._sheet = wsheet_object
		super(PtDatasplitWorkflow, self).__init__(wsheet_object)
		options = [
			{"name": "message_table", "type": "infile", "format": "paternity_test.tab"},  # 拆分需要的数据表
			{"name": "data_dir", "type": "infile", "format": "paternity_test.tab"},  # 拆分需要的下机数据压缩文件夹

			{"name": "family_table", "type": "infile", "format": "paternity_test.tab"},  # 需要存入mongo里面的家系信息表
			{"name": "pt_data_split_id", "type": "string"},
			{"name": "member_id", "type": "string"},
			{"name": "update_info", "type": "string"}
		]
		self.add_option(options)
		self.data_split = self.add_tool("paternity_test.data_split")
		self.merge_fastq = self.add_tool("paternity_test.merge_fastq")
		self.set_options(self._sheet.options())
		# self.step.add_steps("data_split", "merge_fastq")
		self.update_status_api = self.api.pt_update_status
		self.tools = []
		self.sample_name_wq = []
		self.sample_name_ws = []
		self.sample_name_un = []
		self.data_dir = ''
		self.wq_dir = ''
		self.done_wq = ''

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

	# def set_step(self, event):
	# 	if 'start' in event['data'].keys():
	# 		event['data']['start'].start()
	# 	if 'end' in event['data'].keys():
	# 		event['data']['end'].finish()
		# self.step.update()

	# def finish_update(self, event):
	# 	step = getattr(self.step, event['data'])
	# 	step.finish()
		# self.step.update()

	def run_data_split(self):
		self.db_customer()  # 家系表导表
		self.data_split.set_options({
			"message_table": self.option('message_table'),
			"data_dir": self.option('data_dir'),
		})
		self.data_split.on('end', self.set_output, 'data_split')
		self.data_split.on('end', self.run_merge_fastq_wq)
		# self.data_split.on('start', self.set_step, {'start': self.step.data_split})
		# self.data_split.on('end', self.set_step, {'end': self.step.data_split})
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
		self.data_dir = self.data_split.output_dir
		sample_name = os.listdir(self.data_dir)
		for j in sample_name:
			p = re.match('Sample_WQ([0-9].*)-(.*)', j)
			q = re.match('Sample_WS-(.*)', j)
			if p:
				self.sample_name_wq.append(j)
			elif q:
				self.sample_name_ws.append(j)
			else:
				self.sample_name_un.append(j)
		n = 0
		self.tools = []
		for i in self.sample_name_wq:
			merge_fastq = self.add_tool("paternity_test.merge_fastq")
			# self.step.add_steps('merge_fastq{}'.format(n))
			merge_fastq.set_options({
				"sample_dir_name": i,
				"data_dir": self.data_dir
			})
			# step = getattr(self.step, 'merge_fastq{}'.format(n))
			# step.start()
			# merge_fastq.on('end', self.finish_update, 'merge_fastq{}'.format(n))
			self.tools.append(merge_fastq)
			n += 1
		for j in range(len(self.tools)):
			self.tools[j].on('end', self.set_output, 'merge_fastq')
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
		for i in self.sample_name_ws:
			merge_fastq = self.add_tool("paternity_test.merge_fastq")
			merge_fastq.set_options({
				"sample_dir_name": i,
				"data_dir": self.data_dir
			})
			self.tools.append(merge_fastq)
			n += 1
		for j in range(len(self.tools)):
			self.tools[j].on('end', self.set_output, 'merge_fastq')
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
			"name": "paternity_test.pt_batch",
			"instant": False,
			"IMPORT_REPORT_DATA": True,
			"IMPORT_REPORT_AFTER_END": False,
			"options": {
				"member_id": self.option('member_id'),
				"fastq_path": self.wq_dir,
				"cpu_number": 8,
				"ref_fasta": "/mnt/ilustre/users/sanger-dev/sg-users/xuanhongdong/db/genome/human/hg38.chromosomal_assembly/ref.fa",
				"targets_bedfile": "/mnt/ilustre/users/sanger-dev/sg-users/xuanhongdong/share/pt/snp.chr.sort.3.bed",
				"ref_point": "/mnt/ilustre/users/sanger-dev/sg-users/zhoumoli/pt/targets.bed.rda",
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
		return 'pt.med_report_tupdate'

	def run_merge_fastq_un(self):
		if self.done_wq != "true":
			self.run_wq_wf()
		n = 0
		self.tools = []
		for i in self.sample_name_un:
			merge_fastq = self.add_tool("paternity_test.merge_fastq")
			merge_fastq.set_options({
				"sample_dir_name": i,
				"data_dir": self.data_dir
			})
			self.tools.append(merge_fastq)
			n += 1
		for j in range(len(self.tools)):
			self.tools[j].on('end', self.set_output, 'merge_fastq')
		if len(self.tools) > 1:
			self.on_rely(self.tools, self.end)
		else:
			self.tools[0].on('end', self.end)
		for tool in self.tools:
			tool.run()

	def run(self):
		self.run_data_split()
		super(PtDatasplitWorkflow, self).run()

	def set_output(self, event):
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
			n = re.match('WS-(.*)', file_name[0])  # ws
			if m:
				self.linkdir(obj.output_dir, wq_dir)
			else:
				if n:
					self.linkdir(obj.output_dir, ws_dir)
				else:
					self.linkdir(obj.output_dir, undetermined_dir)

	def end(self):
		self.logger.info("医学流程数据拆分结束")
		if self.done_wq != "true":
			self.run_wq_wf()
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

