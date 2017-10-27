# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
# __modified__ = "moli.zhou"
import re
from biocluster.module import Module
import os
import types
from biocluster.core.exceptions import OptionError


class NiptAnalysisModule(Module):
	def __init__(self, work_id):
		super(NiptAnalysisModule, self).__init__(work_id)
		self.step.add_steps('fastq2bed', 'bed_analysis','fastqc','identification')
		options = [
			{"name": "fastq_path", "type": "infile", "format": "sequence.fastq_dir"},
			{"name": "sample_id", "type": "string"},
			{"name": "bw", "type": "int", "default": 10},
			{"name": "bs", "type": "int", "default": 1},
			{"name": "ref_group", "type": "int", "default": 1},
			{"name": "single", "type": "string", "default": "false"}
		]
		self.add_option(options)
		self.fastq2bed = self.add_tool("nipt.fastq_process")
		self._end_info = 0

	def check_options(self):
		"""
		 重写参数检测函数
		 :return:
		 """
		if not self.option('fastq_path').is_set:
			raise OptionError('必须提供fastq文件所在的路径')
		return True

	def set_step(self, event):
		if 'start' in event['data'].keys():
			event['data']['start'].start()
		if 'end' in event['data'].keys():
			event['data']['end'].finish()
		self.step.update()

	def fastq2bed_run(self):
		self.fastq2bed.set_options({
			"sample_id": self.option('sample_id'),
			"fastq_path": self.option("fastq_path"),
			"single":self.option("single")
		}
		)

		self.fastq2bed.on('end', self.set_output,'fastq2bed')
		self.fastq2bed.on('start', self.set_step, {'start': self.step.fastq2bed})
		self.fastq2bed.on('end', self.set_step, {'end': self.step.fastq2bed})
		self.fastq2bed.on('end', self.bed_run)
		self.fastq2bed.run()

	def bed_run(self):
		self.bed_analysis = self.add_tool("nipt.bed_analysis")
		# bed_dir = os.path.join(self.work_dir, "FastqProcess/output")
		bed_dir = self.fastq2bed.output_dir
		self.bed_analysis.set_options({
			"bed_file": bed_dir+'/'+ self.option('sample_id')+'.bed.2',
			"bw": self.option('bw'),
			'bs':self.option('bs'),
			'ref_group':self.option('ref_group'),
			"single_chr": "false"
		})
		self.bed_analysis.on('end', self.set_output,'bed_analysis')
		self.bed_analysis.on('start', self.set_step, {'start': self.step.bed_analysis})
		self.bed_analysis.on('end', self.set_step, {'end': self.step.bed_analysis})
		self.bed_analysis.on('end', self.fastqc_run)
		self.bed_analysis.run()

	def fastqc_run(self):
		self.fastqc = self.add_tool("nipt.fastqc")
		# bed_dir = os.path.join(self.work_dir, "FastqProcess/output")
		bed_dir = self.fastq2bed.output_dir
		self.fastqc.set_options({
			"sample_id":self.option('sample_id'),
			"fastq_path":self.option('fastq_path'),
			"bam_file": bed_dir+'/'+ self.option('sample_id') + '.map.valid.bam',
		}
		)

		self.fastqc.on('end', self.set_output,'fastqc')
		self.fastqc.on('start', self.set_step, {'start': self.step.fastqc})
		self.fastqc.on('end', self.set_step, {'end': self.step.fastqc})
		self.fastqc.on('end', self.identification_run)
		self.fastqc.run()

	def identification_run(self):
		self.identification = self.add_tool("nipt.bed_analysis")
		# bed_dir = os.path.join(self.work_dir, "FastqProcess/output")
		bed_dir = self.fastq2bed.output_dir
		self.identification.set_options({
			"bed_file": bed_dir + '/' + self.option('sample_id') + '.bed.2',
			"bw": 500,
			'bs': 500,
			'ref_group': self.option('ref_group'),
			"single_chr": 'true'
		})
		self.identification.on('end', self.set_output, 'identification')
		self.identification.on('start', self.set_step, {'start': self.step.bed_analysis})
		self.identification.on('end', self.set_step, {'end': self.step.bed_analysis})
		self.identification.on('end', self.end)
		self.identification.run()


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
					# self.logger.info('rm -r %s' % newfile)
		for i in range(len(allfiles)):
			if os.path.isfile(oldfiles[i]):
				os.link(oldfiles[i], newfiles[i])
			elif os.path.isdir(oldfiles[i]):
				# self.logger.info('cp -r %s %s' % (oldfiles[i], newdir))
				os.system('cp -r %s %s' % (oldfiles[i], newdir))

	def set_output(self, event):
		obj = event['bind_object']
		if event['data'] == 'fastq2bed' or event['data'] == 'bed_analysis' or event['data'] == 'fastqc' or event['data']\
				== 'identification':
			allfiles = os.listdir(obj.output_dir)
			oldfiles = [os.path.join(obj.output_dir, i) for i in allfiles]
			newfiles = [os.path.join(self.output_dir, i) for i in allfiles]
			for i in range(len(allfiles)):
				os.link(oldfiles[i], newfiles[i])


	def run(self):
		super(NiptAnalysisModule, self).run()
		self.fastq2bed_run()

	def end(self):
		repaths = [
			[".", "", "无创产前筛查结果输出目录"],
		]

		sdir = self.add_upload_dir(self.output_dir)
		sdir.add_relpath_rules(repaths)
		super(NiptAnalysisModule, self).end()