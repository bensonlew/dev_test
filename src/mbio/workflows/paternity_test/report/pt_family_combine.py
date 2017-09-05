# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'

'''医学检验所-无创产前亲子鉴定家系自由组合模块'''
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import re
from bson import ObjectId
from biocluster.config import Config
import json
import shutil


class PtFamilyCombineWorkflow(Workflow):
	"""
	last_modify 20170901
	"""
	def __init__(self, wsheet_object):
		'''
		:param wsheet_object:
		'''
		self._sheet = wsheet_object
		super(PtFamilyCombineWorkflow, self).__init__(wsheet_object)
		options = [
			{"name": "dad_id", "type": "string"},  # 输入F/M/S的样本ID
			{"name": "mom_id", "type": "string"},
			{"name": "preg_id", "type": "string"},
			{"name": "err_min", "type": "int", "default": 2},  # 允许错配数
			{"name": "dad_group", "type": "string"},
			{"name": "update_info", "type": "string"},
			{"name": 'new_mom_id', "type": "string"},
			{"name": 'new_dad_id', "type": "string"},
			{"name": 'new_preg_id', "type": "string"},
			{"name": 'dedup_start', "type": "string"},
			{"name": 'dedup_end', "type": "string"},
			{"name": 'dedup_all', "type": "string"},
			{"name": 'main_id', "type": "string"},   # 主表id
			{"name": 'member_id', "type": "string"},
		]
		self.add_option(options)
		self.ref_dir = self.config.SOFTWARE_DIR
		self.tab_file = self.api.tab_file
		self.sg_pt = self.api.sg_paternity_test
		self.ref_data = self.config.SOFTWARE_DIR + "/database/human/pt_ref/tab_data/"
		self.set_options(self._sheet.options())
		self.tools_analysis = []
		self.tools_result = []
		self.tools_dedup = []

	def check_options(self):
		'''
		参数检查
		:return:
		'''
		if not self.option("dad_group"):
			raise OptionError("必须输入父本组编号")
		if not self.option("mom_id"):
			raise OptionError("必须输入母本编号")
		if not self.option("preg_id"):
			raise OptionError("必须输入胎儿编号")
		if self.option('new_dad_id') and self.option('new_mom_id') and self.option('new_preg_id'):
			if not self.option('dad_id'):
				raise OptionError("必须输入父本编号，方便针对性替换")
		return True

	"""
	def cat_sample_run(self):
		self.cat_sample.set_options({
			"sample_1": self.preg_list[0],
			"sample_2": self.preg_list[1],
		})
		self.cat_sample.run()

	def fastq2mongo_dc_run(self):
		i = self.preg_list[0] + "_" + self.preg_list[1]
		self.fastq2mongo_dc.set_options({
			"sample_id": i,
			"fastq_path": self.cat_sample.option("fastq_path"),
			"cpu_number": 4,
			"ref_fasta": self.ref_dir + "/database/human/hg38.chromosomal_assembly/ref.fa",
			"targets_bedfile": self.ref_dir + "/database/human/pt_ref/snp.chr.sort.3.bed",
			"type": 'pt'
		})
		self.fastq2mongo_dc.on('end', self.set_output, 'fastq2mongo')
		self.fastq2mongo_dc.run()
	"""

	def pt_analysis_run(self):
		for i in range(len(self.family)):
			pt_analysis = self.add_module("paternity_test.pt_analysis")
			dad_id = self.family[i][0]
			mom_id = self.family[i][1]
			preg_id = self.family[i][2]
			pt_analysis.set_options({
				"dad_tab": self.output_dir + '/' + dad_id + '.tab',
				"mom_tab": self.output_dir + '/' + mom_id + '.tab',
				"preg_tab": self.output_dir + '/' + preg_id + '.tab',
				"ref_point": self.ref_dir + "/database/human/pt_ref/targets.bed.rda",
				"err_min": self.option("err_min")
			})
			self.tools_analysis.append(pt_analysis)
		for j in range(len(self.tools_analysis)):
			self.tools_analysis[j].on('end', self.set_output, 'pt_analysis')
		if len(self.tools_analysis) > 1:
			self.on_rely(self.tools_analysis, self.result_info_run)
		elif len(self.tools_analysis) == 1:
			self.tools_analysis[0].on('end', self.result_info_run)
		for t in self.tools_analysis:
			t.run()

	def result_info_run(self):
		if len(self.family) == 1:
			result_dir = self.work_dir + "/PtAnalysis/FamilyMerge/output"
		else:
			result_dir = self.output_dir
		results = os.listdir(result_dir)
		for f in results:
			if re.match(r'.*family_joined_tab\.Rdata$', f):
				result_info = self.add_tool("paternity_test.result_info")
				self.rdata = os.path.join(result_dir, f)
				result_info.set_options({
					"tab_merged": self.rdata
				})
				self.tools_result.append(result_info)
			else:
				pass
		for j in range(len(self.tools_result)):
			self.tools_result[j].on('end', self.set_output, 'result_info')
		if len(self.tools_result) > 1:
			if not self.option('dedup_start') and self.option('dedup_all') == 'false':
				self.on_rely(self.tools_result, self.end)
			else:
				self.on_rely(self.tools_result, self.dedup_run)
		elif len(self.tools_result) == 1:
			if not self.option('dedup_start') and self.option('dedup_all') == 'false':
				self.tools_result[0].on('end', self.end)
			else:
				self.on_rely(self.tools_result, self.dedup_run)
		for t in self.tools_result:
			t.run()

	def dedup_run(self):
		"""
		查重部分新机制
		:return:
		"""

		father_data = self.output_dir + "/" + "father"  # 自定义查重父本
		if not os.path.exists(father_data):
			os.mkdir(father_data)
		if self.option('dedup_all') != "true":
			api_read_tab = self.api.tab_file
			temp_s = re.match('WQ([1-9].*)', self.option('dedup_start'))
			temp_e = re.match('WQ([1-9].*)', self.option('dedup_end'))
			num_list = range(int(temp_s.group(1)), int(temp_e.group(1)))
			# num = int(temp_s.group(1))
			# num_list = range(num-int(self.option('dedup_num')), num+int(self.option('dedup_num'))+1)
			name_list = []
			for m in num_list:
				x = api_read_tab.dedup_sample_report(m)  # 这边主要取的是 同一个家系下的所有父本
				if len(x): #如果库中能取到前后的样本
					for k in range(len(x)):
						name_list.append(x[k])
			name_list = list(set(name_list))
			self.logger.info("name_list:%s" % name_list)
			for m in name_list:
				old_path = os.path.join(self.ref_data, m + '.tab')
				new_path = os.path.join(father_data, m + '.tab')
				if os.path.exists(old_path):
					os.link(old_path, new_path)
				else:
					self.logger.info('参考库中暂时没有这个样本——{}'.format(m))
				# api_read_tab.export_tab_file(str(m), father_data)
			for i in range(len(self.family)):
				mom_id = self.family[i][1]
				preg_id = self.family[i][2]
				dad_id = self.family[i][0]
				pt_analysis_dedup = self.add_tool("paternity_test.dedup")
				pt_analysis_dedup.set_options({
					"mom_tab": self.output_dir + '/' + mom_id + '.tab',
					"preg_tab": self.output_dir + '/' + preg_id + '.tab',
					"ref_point": self.ref_dir + "/database/human/pt_ref/targets.bed.rda",
					"err_min": self.option("err_min"),
					"father_path": father_data + "/",
					"dad_id": dad_id
				})
				self.tools_dedup.append(pt_analysis_dedup)
		else:
			for i in range(len(self.family)):
				mom_id = self.family[i][1]
				preg_id = self.family[i][2]
				dad_id = self.family[i][0]
				pt_analysis_dedup = self.add_tool("paternity_test.dedup")
				pt_analysis_dedup.set_options({
					"mom_tab": self.output_dir + '/' + mom_id + '.tab',
					"preg_tab": self.output_dir + '/' + preg_id + '.tab',
					"ref_point": self.ref_dir + "/database/human/pt_ref/targets.bed.rda",
					"err_min": self.option("err_min"),
					"father_path": self.ref_data,
					"dad_id": dad_id
				})
				self.tools_dedup.append(pt_analysis_dedup)
		for j in range(len(self.tools_dedup)):
			self.tools_dedup[j].on('end', self.set_output, 'dedup')
		if len(self.tools_dedup) > 1:
			self.on_rely(self.tools_dedup, self.end)
		else:
			self.tools_dedup[0].on("end", self.end)
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
		if event['data'] == "fastq2mongo":
			tab_name = self.option('preg_id') + '.mem.sort.hit.vcf.tab'
			self.linkdir(obj.output_dir + '/fastq2tab', self.output_dir)
			new_tab_name = self.option('preg_id') + '.tab'
			os.link(obj.output_dir + '/fastq2tab/' + tab_name, os.path.join(self.output_dir, new_tab_name))
		if event['data'] == "pt_analysis":
			print obj.output_dir
			self.linkdir(obj.output_dir +'/family_analysis', self.output_dir)
			self.linkdir(obj.output_dir + '/family_merge', self.output_dir)
		if event['data'] == "result_info":
			self.linkdir(obj.output_dir, self.output_dir)
		if event['data'] == "dedup":
			self.linkdir(obj.output_dir, self.output_dir)

	def run(self):
		self.family = []
		if self.option('dad_id'):
			combine = []
			old_mom_id = self.option('mom_id')
			old_dad_id = self.option('dad_group') + '-' + self.option('dad_id')
			old_preg_id = self.option('preg_id')
			if self.option('new_dad_id'):  # 如果存在这个参数，表示后面的流程中要修改相应的tab文件的名字，替换完成自由交互出报告的功能
				self.mom_id = self.option('new_mom_id')  # 新的家系的各个样本的id名称
				self.dad_id = self.option('new_dad_id')
				self.preg_id = self.option('new_preg_id')
				combine.append(self.dad_id)
				combine.append(self.mom_id)
				combine.append(self.preg_id)
				self.tab_file.export_tab_file(old_mom_id, self.output_dir, self.mom_id)
				self.tab_file.export_tab_file(old_dad_id, self.output_dir, self.dad_id)
				self.tab_file.export_tab_file(old_preg_id, self.output_dir, self.preg_id)
			else:
				combine.append(old_dad_id)
				combine.append(old_mom_id)
				combine.append(old_preg_id)
				self.tab_file.export_tab_file(old_mom_id, self.output_dir)
				self.tab_file.export_tab_file(old_dad_id, self.output_dir)
				self.tab_file.export_tab_file(old_preg_id, self.output_dir)
			self.family.append(combine)
		else:
			dad_list = self.tab_file.find_father_id(self.option('dad_group'))
			self.tab_file.export_tab_file(self.option('mom_id'), self.output_dir)
			self.tab_file.export_tab_file(self.option('preg_id'), self.output_dir)
			for i in dad_list:
				combine = []
				combine.append(i)
				combine.append(self.option('mom_id'))
				combine.append(self.option('preg_id'))
				self.family.append(combine)
				self.tab_file.export_tab_file(i, self.output_dir)
		self.logger.info(self.family)
		self.pt_analysis_run()
		"""
		else:
			self.cat_sample = self.add_tool('paternity_test.cat_sample')
			self.fastq2mongo_dc = self.add_module("paternity_test.fastq2mongo_dc")
			self.cat_sample.on('end', self.fastq2mongo_dc_run)
			self.fastq2mongo_dc.on('end', self.pt_analysis_run)
			self.pt_analysis.on('end', self.result_info_run)
			self.result_info.on('end', self.dedup_run)
			self.cat_sample_run()
		"""
		super(PtFamilyCombineWorkflow, self).run()

	def end(self):
		self.logger.info("开始end函数")
		results = os.listdir(self.output_dir)
		api_main = self.api.sg_paternity_test
		if self.option('dedup_all') == 'true':
			dedup = 'all'
		else:
			if self.option('dedup_start'):
				dedup = self.option('dedup_start') + '-' + self.option('dedup_end')
			else:
				dedup = 'no'
		for i in range(len(self.family)):
			dad_id = self.family[i][0]
			mom_id = self.family[i][1]
			preg_id = self.family[i][2]
			self.father_id = api_main.add_sg_father(dad_id, mom_id, preg_id, self.option('main_id'), self.option("member_id"))
			# 此处的main_id相当于别处的batch_id为该自由交互的主表，和正式流程里的不一样

			self.pt_father_id = api_main.add_pt_father(father_id=self.father_id, err_min=self.option('err_min'), dedup=dedup)  # 交互表id
			dedup = '.*' + mom_id + '_' + preg_id + '_family_analysis.txt'
			dedup_new = dad_id + "_" + mom_id + '_' + preg_id + '.txt'
			for f in results:
				if re.search(dedup, f):
					api_main.add_analysis_tab(self.output_dir + '/' + f, self.pt_father_id)
				elif f == dad_id + '_' + mom_id + '_' + preg_id + '_family_joined_tab.txt':
					api_main.add_sg_pt_father_detail(self.output_dir + '/' + f, self.pt_father_id)
				elif f == mom_id + '_' + preg_id + '_info_show.txt':
					api_main.add_info_detail(self.output_dir + '/' + f, self.pt_father_id)
				elif f == dad_id + '_' + mom_id + '_' + preg_id + '_test_pos.txt':
					api_main.add_test_pos(self.output_dir + '/' + f, self.pt_father_id)
				elif f == dad_id + '_' + mom_id + '_' + preg_id + '_family.png':
					file_dir = self.output_dir + '/' + dad_id + '_' + mom_id + '_' + preg_id
					api_main.add_pt_father_figure(file_dir, self.pt_father_id)
				elif str(f) == str(dedup_new):
					self.logger.info(f)
					api_main.import_dedup_data(self.output_dir + '/' + f, self.pt_father_id)

			self.update_status_api = self.api.pt_update_status
			self.update_status_api.add_pt_status(table_id=self.option('pt_father_id'), table_name='sg_pt_father',
			                                     type_name='sg_pt_father')
		super(PtFamilyCombineWorkflow, self).end()
