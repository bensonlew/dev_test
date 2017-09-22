# -*- coding: utf-8 -*-
"""
Created on Fri Sep 08 15:27:18 2017

@author: Lotus
"""
#import urllib
from pymongo import MongoClient
import os
from biocluster.tool import Tool
from biocluster.agent import Agent
import random
import time
from bson.objectid import ObjectId

_jobID=None
class MethTracerAgent(Agent):
	def __init__(self, parent):
		super(MethTracerAgent,self).__init__(parent)
		options = [
			{"name":"path","type":"string","default":None},
			{"name":"model","type":"string","default":"MLE"},
			#{"name":"email","type":"string","default":None},
			{"name":"jobID","type":"string","default":None},
			]

		self.add_option(options)

		_jobID = self.option('jobID')
		self.logger.info(_jobID)
		
	def getPath(self):		
		return self.option("path")
	
	def getModel(self):
		return self.option("model")
	
	def getEmail(self):
		return self.option("email")
	
	def check_options(self):
		if not self.option('path'):
			raise OptionError("parameter path must be provided!")
		else:
			_path = self.option('path')
			self.logger.info("path:%s" % self.option("path"))
		if not self.option('model'):
			raise OptionError("parameter model must be provided!")
		else:
			_model = self.option("model")
			self.logger.info("model:%s" % self.option("model"))
	
	def set_resource(self):
		self._cpu = 1
		self._memory = '2G'
		
	def end(self):
		super(MethTracerAgent,self).end()

class MethTracerTool(Tool):
	def __init__(self, config):
		super(MethTracerTool, self).__init__(config)
		self.Python_path = 'program/Python/bin/python '
		self.cmd_path = self.config.SOFTWARE_DIR + '/ctMethTracer.py'
		#self.script_path = "bioinfo/meta/scripts/beta_diver.sh"
		self.R_path = os.path.join(self.config.SOFTWARE_DIR, 'program/R-3.3.1/bin/R')

	def run(self):
		super(MethTracerTool, self).run()
		self.run_MethTracerTool()
		self.end()


	def run_MethTracerTool(self):
		#_cmd = self.cmd_path
		
		#log_file = self.work_dir + '/cmd.log'
		#_cmd = self.Python_path + self.cmd_path + ' -jobID %s -log %s' % (self.option('jobID'), log_file)
		_cmd = self.Python_path + self.cmd_path + ' -jobID %s ' % self.option('jobID')
		self.logger.info("run ctMethTracer.py")

		_cmd1 = self.add_command("meth_tracer.sh", _cmd).run()

		_path = self.option("path")
		_model = self.option("model")
		_id = self.option('jobID')
		_client = MongoClient('10.100.200.131',27017)

		#_db = _client.ctMethTracer
		#_job = _db.job
		#_posts = {
		#		  'step':0,
		#		  'status':'test',
		#		  'description':None,
		#		  'params':{
		#				'input':_path,
		#				'model':_model,
		#		  },
		#		  'email':'123@127.com'
		#	  }
		#_jobID = _job.insert_one(_posts).inserted_id
		
		
