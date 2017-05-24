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
			{"name": "batch_id", "type": "string"},
			{'name': 'member_id','type':'string'},
			{"name": "bw", "type": "int", "default": 10},
			{"name": "bs", "type": "int", "default": 1},
			{"name": "ref_group", "type": "int", "default": 2}
		]
		self.add_option(options)
		self.set_options(self._sheet.options())
		self.tools=[]
		self.tool_bed = []
		self.tool_fastqc =[]
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

	def fastq_run(self):
		self.api_nipt = self.api.nipt_analysis
		file = self.option('customer_table').prop['path']
		self.api_nipt.nipt_customer(file)
		os.listdir(self.option('fastq_path').prop['path'])

		n = 0
		for i in os.listdir(self.option('fastq_path').prop['path']):
			m = re.match('(.*)_R1.fastq.gz', i)
			if m:
				self.sample_id.append(m.group(1))
				fastq_process = self.add_tool("nipt.fastq_process")
				self.step.add_steps('fastq_process{}'.format(n))
				fastq_process.set_options({
					"sample_id": m.group(1),
					"fastq_path": self.option("fastq_path")
				}
				)
				step = getattr(self.step, 'fastq_process{}'.format(n))
				step.start()
				fastq_process.on('end', self.finish_update, 'fastq_process{}'.format(n))
				self.tools.append(fastq_process)
				n += 1

		for name in self.sample_id:
			self.main_id = self.api_nipt.add_main(self.option('member_id'), name, self.option('batch_id'))
			self.api_nipt.add_interaction(self.main_id, self.option('bw'), self.option('bs'), self.option('ref_group'))

		for j in range(len(self.tools)):
			self.tools[j].on('end', self.set_output)

		if len(self.tools) > 1:
			self.on_rely(self.tools, self.bed_run)
		elif len(self.tools) == 1:
			self.tools[0].on('end', self.bed_run)

		for tool in self.tools:
			tool.run()


	def bed_run(self):
		n = 0
		for i in self.sample_id:
			bed_analysis = self.add_tool("nipt.bed_analysis")
			self.step.add_steps('bed_analysis{}'.format(n))
			bed_analysis.set_options({
				"bed_file": self.output_dir+'/'+i+'_R1.bed.2',
				"bw": self.option('bw'),
				'bs':self.option('bs'),
				'ref_group':self.option('ref_group')
			}
			)
			step = getattr(self.step, 'bed_analysis{}'.format(n))
			step.start()
			bed_analysis.on('end', self.finish_update, 'bed_analysis{}'.format(n))
			self.tool_bed.append(bed_analysis)
			n += 1

		for j in range(len(self.tool_bed)):
			self.tool_bed[j].on('end', self.set_output)

		if len(self.tool_bed) > 1:
			self.on_rely(self.tool_bed, self.fastqc)
		elif len(self.tool_bed) == 1:
			self.tool_bed[0].on('end', self.fastqc)

		for tool in self.tool_bed:
			tool.run()

	def fastqc(self):
		n = 0
		for i in self.sample_id:
			fastqc = self.add_tool("nipt.fastqc")
			self.step.add_steps('fastqc{}'.format(n))
			fastqc.set_options({
				"sample_id":i,
				"fastq_path":self.option('fastq_path'),
				"bed_file": self.output_dir + '/' + i + '_R1.bed.2',
			}
			)
			step = getattr(self.step, 'fastqc{}'.format(n))
			step.start()
			fastqc.on('end', self.finish_update, 'fastqc{}'.format(n))
			self.tool_fastqc.append(fastqc)
			n += 1

		for j in range(len(self.tool_fastqc)):
			self.tool_fastqc[j].on('end', self.set_output)

		if len(self.tool_fastqc) > 1:
			self.on_rely(self.tool_fastqc, self.end)
		elif len(self.tool_fastqc) == 1:
			self.tool_fastqc[0].on('end', self.end)

		for tool in self.tool_fastqc:
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
		self.linkdir(obj.output_dir, self.output_dir)

	def run(self):
		self.fastq_run()
		super(NiptWorkflow, self).run()

	def end(self):
		self.api_nipt.add_fastqc(self.main_id, self.output_dir)  # fastqc入库
		for i in os.listdir(self.output_dir):
			if re.search(r'.*bed.2$', i):
				self.api_nipt.add_bed_file(self.output_dir + '/'+ i)
			elif re.search(r'.*qc$', i):
				self.api_nipt.add_qc(self.output_dir + '/' + i)
			elif re.search(r'.*z.xls$', i):
				self.api_nipt.add_z_result(self.output_dir + '/' + i,self.main_id)
			elif re.search(r'.*zz.xls$', i):
				self.api_nipt.add_z_result(self.output_dir + '/' + i, self.main_id)
		super(NiptWorkflow, self).end()
