# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
#last modified:20161220

'''医学检验所-无创产前亲子鉴定流程'''
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import re
import json
import shutil

class PtReportWorkflow(Workflow):
	def __init__(self, wsheet_object):
		'''
		:param wsheet_object:
		'''
		self._sheet = wsheet_object
		super(PtReportWorkflow, self).__init__(wsheet_object)
		options = [
			{"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"},  # 参考序列
			{"name": "targets_bedfile", "type": "string"},

			{"name": "dad_id", "type": "string"},  # 输入F/M/S的样本ID
			{"name": "mom_id", "type": "string"},
			{"name": "preg_id", "type": "string"},
			{"name": "err_min", "type": "int", "default": 2},  # 允许错配数
			{"name": "ref_point", "type": "string"},  # 参考位点
			{"name": "dedup_num", "type": "int", "default": 50},  # 查重样本数
			{"name": "flow_id", "type": "string"},  # 查重样本数
		]
		self.add_option(options)
		self.pt_analysis = self.add_module("paternity_test.pt_analysis")
		self.result_info = self.add_tool("paternity_test.result_info")
		self.tools = []
		self.tools_rename = []
		self.tools_rename_analysis = []
		self.tools_dedup =[]
		self.tools_dedup_f = []
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


	def pt_analysis_run(self):
		api_read_tab = self.api.tab_file
		print type(api_read_tab.export_tab_file(str(self.option('dad_id')), self.output_dir))
		self.pt_analysis.set_options({
			"dad_tab": api_read_tab.export_tab_file(str(self.option('dad_id')), self.output_dir),  # 数据库的tab文件
			"mom_tab": api_read_tab.export_tab_file(str(self.option('mom_id')), self.output_dir),
			"preg_tab": api_read_tab.export_tab_file(str(self.option('preg_id')), self.output_dir),
			"ref_point": self.option("ref_point"),
			"err_min": self.option("err_min")
		})
		self.pt_analysis.on('end', self.set_output, 'pt_analysis')
		self.pt_analysis.on('end', self.result_info_run)
		self.pt_analysis.on('start', self.set_step, {'start': self.step.pt_analysis})
		self.pt_analysis.on('end', self.set_step, {'end': self.step.pt_analysis})
		self.pt_analysis.run()

	def result_info_run(self):
		self.result_info.set_options({
			"tab_merged":  self.output_dir+'/family_joined_tab.Rdata'
		})
		self.result_info.on('end', self.set_output, 'result_info')
		self.result_info.on('end', self.dedup_run)
		self.result_info.on('start', self.set_step, {'start': self.step.result_info})
		self.result_info.on('end', self.set_step, {'end': self.step.result_info})
		self.result_info.run()

	def dedup_run(self):
		api_read_tab = self.api.tab_file
		n = 0
		temp = re.match('WQ([1-9].*)-F.*', self.option('dad_id'))
		num = int(temp.group(1))
		num_list = range(num-self.option('dedup_num'), num+self.option('dedup_num')+1)
		name_list = []
		for m in num_list:
			x = api_read_tab.dedup_sample(m)
			if len(x): #如果库中能取到前后的样本
				for k in range(len(x)):
					name_list.append(x[k])
		name_list = list(set(name_list))
		for i in name_list:
			if i == self.option('dad_id'):
				continue
			pt_analysis_dedup = self.add_module("paternity_test.pt_analysis")
			self.step.add_steps('dedup_{}'.format(n))
			pt_analysis_dedup.set_options({
					"dad_tab": api_read_tab.export_tab_file(i, self.output_dir),  # 数据库的tab文件
					"mom_tab": api_read_tab.export_tab_file(self.option('mom_id'), self.output_dir),
					"preg_tab": api_read_tab.export_tab_file(self.option('preg_id'), self.output_dir),
					"ref_point": self.option("ref_point"),
					"err_min": self.option("err_min")
			}
			)
			step = getattr(self.step, 'dedup_{}'.format(n))
			step.start()
			pt_analysis_dedup.on('end', self.finish_update, 'dedup_{}'.format(n))
			self.tools_dedup.append(pt_analysis_dedup)
			n += 1
		for j in range(len(self.tools_dedup)):
			self.tools_dedup[j].on('end', self.set_output, 'dedup')
		if len(self.tools_dedup) > 1:
			self.on_rely(self.tools_dedup, self.dedup_fuzzy_run)
		elif len(self.tools_dedup) == 1:
			self.tools_dedup[0].on('end', self.dedup_fuzzy_run)
		else:
			self.dedup_fuzzy_run()
		for tool in self.tools_dedup:
			tool.run()

	def dedup_fuzzy_run(self):
		api_read_tab = self.api.tab_file
		n = 0
		temp = re.match('WQ([1-9].*)-F.*', self.option('dad_id'))
		num = int(temp.group(1))
		if api_read_tab.dedup_fuzzy_sample(num, self.option('dad_id')):
			for i in api_read_tab.dedup_fuzzy_sample(num,self.option('dad_id')):
				pt_analysis_dedup_f = self.add_module("paternity_test.pt_analysis")
				self.step.add_steps('dedup_f_{}'.format(n))
				pt_analysis_dedup_f.set_options({
					"dad_tab": api_read_tab.export_tab_file(i, self.output_dir),  # 数据库的tab文件
					"mom_tab": api_read_tab.export_tab_file(self.option('mom_id'), self.output_dir),
					"preg_tab": api_read_tab.export_tab_file(self.option('preg_id'), self.output_dir),
					"ref_point": self.option("ref_point"),
					"err_min": self.option("err_min")
				}
				)
				step = getattr(self.step, 'dedup_f_{}'.format(n))
				step.start()
				pt_analysis_dedup_f.on('end', self.finish_update, 'dedup_f_{}'.format(n))
				self.tools_dedup_f.append(pt_analysis_dedup_f)
				n += 1
		for j in range(len(self.tools_dedup_f)):
			self.tools_dedup_f[j].on('end', self.set_output, 'dedup_fuzzy')
		if len(self.tools_dedup_f) > 1:
			self.on_rely(self.tools_dedup_f, self.end)
		elif len(self.tools_dedup_f) == 1:
			self.tools_dedup_f[0].on('end', self.end)
		else:
			self.end()
		for tool in self.tools_dedup_f:
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

		if event['data'] == "pt_analysis":
			self.linkdir(obj.output_dir +'/family_analysis', self.output_dir)
			self.linkdir(obj.output_dir + '/family_merge', self.output_dir)
			api_main = self.api.sg_paternity_test
			self.flow_id = api_main.add_pt_task_main(err_min=self.option("err_min"), task=None,flow_id=self.option('flow_id'))

		if event['data'] == "result_info":
			self.linkdir(obj.output_dir, self.output_dir)
			# api_main = self.api.sg_paternity_test
			# api_main.add_pt_figure(obj.output_dir)

		if event['data'] == "dedup":
			self.linkdir(obj.output_dir + '/family_analysis', self.output_dir)

		if event['data'] == "dedup_fuzzy":
			self.linkdir(obj.output_dir + '/family_analysis', self.output_dir)

	def run(self):
		self.pt_analysis_run()
		super(PtReportWorkflow, self).run()



	def end(self):
		api_main = self.api.sg_paternity_test
		api_main.add_sg_pt_family(self.option('dad_id'), self.option('mom_id'), self.option('preg_id'),
		                          self.option('err_min'), self.option('ref_fasta').prop['path'], self.option('targets_bedfile'),
		                          self.option('ref_point'))
		results = os.listdir(self.output_dir)
		for f in results:
			if re.search(r'.*family_analysis\.txt$', f):
				api_main.add_analysis_tab(self.output_dir + '/' + f, self.flow_id)
			if re.search(r'.*family_joined_tab\.txt$', f):
				api_main.add_sg_pt_family_detail(self.output_dir + '/' + f, self.flow_id)
			if re.search(r'.*info_show\.txt$', f):
				api_main.add_info_detail(self.output_dir + '/' + f, self.flow_id)
			if re.search(r'.*test_pos\.txt$', f):
				api_main.add_test_pos(self.output_dir + '/' + f, self.flow_id)
			if f == "family.png":
				api_main.add_pt_figure(self.output_dir, self.flow_id)


		super(PtReportWorkflow,self).end()
