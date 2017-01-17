# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
import web
from ..core.basic import Basic
from mainapp.libs.signature import check_sig
import random
from mainapp.models.workflow import Workflow
from ..core.instant import Instant
from mainapp.models.mongo.submit.paternity_test_mongo import PaternityTest
import json

class PtController(object):

	def __init__(self, instant=False):
		self._post_data = None
		self._sheet_data = None
		self._instant = instant
		self._return_msg = None

	@property
	def data(self):
		return self._post_data
	
	@property
	def sheet_data(self):
		return self._sheet_data

	@property
	def return_msg(self):
		"""
		获取Post数据

		:return:
		"""
		return self._return_msg

	@property
	def instant(self):
		"""
		任务是否是即时计算

		:return: bool
		"""
		return self._instant

	@check_sig
	def POST(self):
		workflow_client = Basic(data=self.sheet_data, instant=self.instant)
		try:
			run_info = workflow_client.run()
			self._return_msg = workflow_client.return_msg
			return run_info
		except Exception,e:
			return json.dumps({"success":False, "info":"运行出错：{}".format(e)})

	def set_sheet_data(self, name, options, module_type="workflow", params=None, to_file=None):

		self._post_data = web.input()
		task_info = PaternityTest().get_query_info(self.data.task_id)
		project_sn = task_info['project_sn']
		new_task_id = self.get_new_id(self.data.task_id)
		self._sheet_data = {
		'id': new_task_id,
		'stage_id':0,
		'name': name,
		'type': module_type,
		'client': self.data.client,
		'project_sn':project_sn,
		'IMPORT_REPORT_DATA': True,
		'instant':True,
		'options': options
		}
		if self.instant:
			self._sheet_data['instant'] = True
		if params:
			self._sheet_data['params'] = params
		return self._sheet_data

	def get_new_id(self, task_id, otu_id=None):
		"""
		根据旧的ID生成新的workflowID，固定为旧的后面用“_”，添加两次随机数或者一次otu_id一次随机数
		"""
		new_id = "{}_{}_{}".format(task_id, random.randint(1000, 10000), random.randint(1, 10000))
		workflow_module = Workflow()
		workflow_data = workflow_module.get_by_workflow_id(new_id)
		if len(workflow_data) > 0:
			return self.get_new_id(task_id, otu_id)
		return new_id
