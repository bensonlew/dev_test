# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
# last modified:201705

'''医学检验所-无创产前筛查流程'''
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
			# {"name": "customer_table", "type": "string"},
			# {"name": "fastq_path", "type": "string"},
			# {"name": "bw", "type": "int", "default": 10},
			# {"name": "bs", "type": "int", "default": 1},
			# {"name": "ref_group", "type": "int", "default": 2}

		]
		self.add_option(options)
		self.set_options(self._sheet.options())

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

	def fastq_run(self):
		api = self.api.nipt_analysis
		file = self.option('customer_table').prop['path']
		api.nipt_customer(file)
		os.listdir(self.option('fastq_path').prop['path'])

		n = 0
		for i in os.listdir(self.option('fastq_path').prop['path']):
			m = re.match('(.*)_R1.fastq.gz', i)
			if m:
				fastq_process = self.add_module("nipt.fastq_process")
				self.step.add_steps('fastq_process{}'.format(n))
				fastq_process.set_options({
					"sample_id": m,
					"fastq_path": self.option("fastq_path")
				}
				)
				step = getattr(self.step, 'fastq_process{}'.format(n))
				step.start()
				fastq_process.on('end', self.finish_update, 'fastq_process{}'.format(n))
				self.tools.append(fastq_process)
				n += 1

		for j in range(len(self.tools)):
			self.tools[j].on('end', self.set_output, 'fastq_process')

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
		if event['data'] == "fastq_process":
			self.linkdir(obj.output_dir, self.output_dir)

	def run(self):
		self.fastq_run()
		super(NiptWorkflow, self).run()

	def end(self):
		super(NiptWorkflow, self).end()
