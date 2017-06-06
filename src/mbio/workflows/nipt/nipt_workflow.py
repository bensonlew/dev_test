# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
# last modified:201705

'''医学检验所-无创产前筛查流程'''
import time
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import re


class NiptWorkflow(Workflow):
	def __init__(self, wsheet_object):
		'''
        :param wsheet_object:
        '''
		self._sheet = wsheet_object
		super(NiptWorkflow, self).__init__(wsheet_object)
		options = [
			{"name": "customer_table", "type": "infile", "format": "nipt.xlsx"},
			{"name": "fastq_path", "type": "infile", "format": "sequence.fastq_dir"},
			{"name": "batch_id", "type": "string"},
			{'name': 'member_id','type':'string'},
			{"name": "bw", "type": "int", "default": 10},
			{"name": "bs", "type": "int", "default": 1},
			{"name": "ref_group", "type": "int", "default": 2}

		]
		self.add_option(options)
		self.set_options(self._sheet.options())
		self.tools=[]
		self.sample_id = []

	def check_options(self):
		'''
        检查参数设置
        '''
		if not self.option('fastq_path').is_set:
			raise OptionError('必须提供fastq文件所在的路径')
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

	def analysis_run(self):
		self.api_nipt = self.api.nipt_analysis
		file = self.option('customer_table').prop['path']
		self.api_nipt.nipt_customer(file)
		os.listdir(self.option('fastq_path').prop['path'])

		n = 0
		for i in os.listdir(self.option('fastq_path').prop['path']):
			m = re.match('(.*)_R1.fastq.gz', i)
			if m:
				self.sample_id.append(m.group(1))
				nipt_analysis = self.add_module("nipt.nipt_analysis")
				self.step.add_steps('nipt_analysis{}'.format(n))
				nipt_analysis.set_options({
					"sample_id": m.group(1),
					"fastq_path": self.option("fastq_path"),
					"bw": self.option('bw'),
					'bs': self.option('bs'),
					'ref_group': self.option('ref_group')
				}
				)
				step = getattr(self.step, 'nipt_analysis{}'.format(n))
				step.start()
				nipt_analysis.on('end', self.finish_update, 'nipt_analysis{}'.format(n))
				self.tools.append(nipt_analysis)
				n += 1

		for name in self.sample_id:
			self.main_id = self.api_nipt.add_main(self.option('member_id'), name, self.option('batch_id'))
			self.api_nipt.add_interaction(self.main_id, self.option('bw'), self.option('bs'), self.option('ref_group'))

		for j in range(len(self.tools)):
			self.tools[j].on('end', self.set_output,'nipt_analysis')

		if len(self.tools) > 1:
			self.on_rely(self.tools, self.end)
		elif len(self.tools) == 1:
			self.tools[0].on('end', self.end)

		for tool in self.tools:
			tool.run()



	def linkdir(self, dirpath, dirname):
		"""
        link一个文件夹下的所有文件到本module的output目录
        :param dirpath: 传入文件夹路径
        :param dirname: 新的文件夹名称
        :return:
        """
		allfiles = os.listdir(dirpath)
		# newdir = os.path.join(self.output_dir, dirname)
		newdir = dirname
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
		if event['data'] =='nipt_analysis':
			allfiles = os.listdir(obj.output_dir)
			oldfiles = [os.path.join(obj.output_dir, i) for i in allfiles]
			newfiles = [os.path.join(self.output_dir, i) for i in allfiles]
			for i in range(len(allfiles)):
				os.link(oldfiles[i], newfiles[i])

	def run(self):
		self.analysis_run()
		super(NiptWorkflow, self).run()

	def end(self):
		super(NiptWorkflow, self).end()

		for i in os.listdir(self.output_dir):
			if re.search(r'.*bed.2$', i):
				self.api_nipt.add_bed_file(self.output_dir + '/'+ i)
			elif re.search(r'.*qc$', i):
				self.api_nipt.add_qc(self.output_dir + '/' + i)
			elif re.search(r'.*_z.xls$', i):
				self.api_nipt.add_z_result(self.output_dir + '/' + i,self.main_id)
			elif re.search(r'.*_zz.xls$', i):
				self.api_nipt.add_zz_result(self.output_dir + '/' + i, self.main_id)
		self.api_nipt.add_fastqc(self.main_id, self.output_dir)  # fastqc入库

