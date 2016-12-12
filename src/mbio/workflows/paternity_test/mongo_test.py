# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
#last modified:20161205

'''医学检验所-无创产前亲子鉴定流程'''
from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
import os
import re
import json
import shutil

class PtProcessWorkflow(Workflow):
	def __init__(self, wsheet_object):
		'''
		:param wsheet_object:
		'''
		self._sheet = wsheet_object
		super(PtProcessWorkflow, self).__init__(wsheet_object)
		options = [
			{"name": "sample_id", "type": "string"},  # 输入F/M/S的样本ID
			{"name": "fastq_path", "type": "string"},  # fastq所在路径
			{"name": "cpu_number", "type": "int", "default": 4},  # cpu个数
			{"name": "ref_fasta", "type": "infile", "format": "sequence.fasta"},  # 参考序列
			{"name": "targets_bedfile", "type": "string"}
		]
		self.add_option(options)

		self.tools = []
		self.set_options(self._sheet.options())

	def check_options(self):
		'''
		检查参数设置
		'''

		if not self.option("ref_fasta").is_set:
			raise OptionError("必须输入参考基因组的fastq文件")
		if not self.option('fastq_path'):
			raise OptionError('必须提供fastq文件所在的路径')
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

	def fastq2tab_run(self):
		# api_read_tab = self.api.paternity_test
		file = self.option('sample_id')
		# if not api_read_tab.id_exist(self.option('')):
		#
		# else:
		# 	print "该样本已存在于数据库中"
		n = 0
		for i in file:
			fastq2tab = self.add_module("paternity_test.fastq2tab")
			self.step.add_steps('fastq2tab{}'.format(n))
			fastq2tab.set_options({
				"sample_id": i,
				"fastq_path": self.option("fastq_path"),
				"cpu_number": self.option("cpu_number"),
				"ref_fasta": self.option("ref_fasta"),
				"targets_bedfile": self.option("targets_bedfile"),
			}
			)
			step = getattr(self.step, 'fastq2tab{}'.format(n))
			step.start()
			fastq2tab.on('end', self.finish_update, 'fastq2tab{}'.format(n))
			self.tools.append(fastq2tab)
			n += 1
		for j in range(len(self.tools)):
			self.tools[j].on('end', self.set_output,'fastq2tab')
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
				# self.logger.info('rm -r %s' % newfile)
		for i in range(len(allfiles)):
			if os.path.isfile(oldfiles[i]):
				os.link(oldfiles[i], newfiles[i])
			elif os.path.isdir(oldfiles[i]):
				# self.logger.info('cp -r %s %s' % (oldfiles[i], newdir))
				os.system('cp -r %s %s' % (oldfiles[i], newdir))

	def move2outputdir(self, olddir, newname, mode='link'):  # add by shenghe 20160329
		"""
		移动一个目录下的所有文件/文件夹到workflow输出文件夹下，如果文件夹名已存在，文件夹会被完整删除。
		"""
		if not os.path.isdir(olddir):
			raise Exception('需要移动到output目录的文件夹不存在。')
		newdir = os.path.join(self.output_dir, newname)
		# if os.path.exists(newdir):
		# 	if os.path.islink(newdir):
		# 		os.remove(newdir)
		# 	else:
		# 		shutil.rmtree(newdir)  # 不可以删除一个链接
		if mode == 'link':
			# os.symlink(os.path.abspath(olddir), newdir)  # 原始路径需要时绝对路径
			shutil.copyfile(olddir, newdir, symlinks=True)
		elif mode == 'copy':
			shutil.copyfile(olddir, newdir)
		else:
			raise Exception('错误的移动文件方式，必须是\'copy\'或者\'link\'')

	def set_output(self, event):
		obj = event["bind_object"]
		self.linkdir(obj.output_dir + '/bam2tab', self.output_dir)
		api = self.api.tab_file

		temp = os.listdir(self.output_dir)
		for i in temp:
			if re.search(r'.*.tab$', i):
				tab_path = self.output_dir +'/' + i
		api.add_sg_pt_tab_detail(tab_path)

	def run(self):
		self.fastq2tab_run()
		self.on_rely(self.tools,self.end)
		super(PtProcessWorkflow,self).run()

	def end(self):
		super(PtProcessWorkflow,self).end()
