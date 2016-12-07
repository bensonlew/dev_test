# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
from mainapp.libs.input_check import meta_check
from mainapp.models.mongo.public.meta.meta import Meta
from mainapp.libs.signature import check_sig
from mainapp.models.mongo.distance_matrix import Distance
import random
from mainapp.models.workflow import Workflow
from ..core.instant import Instant
import json


class MetaController(object):

    def __init__(self):
        self._post_data = None
        self._sheet_data = None

    @property
    def data(self):
        """
        获取Post数据

        :return:
        """
        return self._post_data

    @property
    def sheet_data(self):
        """
        获取运行流程所需的Json数据

        :return:
        """
        return self._sheet_data

    @check_sig
    @meta_check
    def POST(self):
        instant_work = Instant(data=self.sheet_data)
        try:
            instant_work.run()
            return instant_work.return_msg
        except Exception, e:
            return json.dumps({"success": False, "info": "运行出错: %s" % e })

    def set_sheet_data(self, name, options, module_type="workflow", params=None, to_file=None):
        """
        设置运行所需的Json文档

        :param name:
        :param module_type:
        :param options:
        :param params:
        :param to_file:
        :return:
        """
        self._post_data = web.input()
        if hasattr(self.data, 'otu_id'):
            otu_id = self.data.otu_id
            table_info = Meta().get_otu_table_info(otu_id)
        else:
            distance_id = self.data.specimen_distance_id
            table_info = Distance().get_distance_matrix_info(distance_id)
        project_sn = table_info["project_sn"]
        task_id = table_info["task_id"]
        self._sheet_data = {
            'id': task_id,
            'stage_id': 0,
            'name': name,  # 需要配置
            'type': module_type,  # 可以配置
            'client': self.data.client,
            'project_sn': project_sn,
            'IMPORT_REPORT_DATA': True,
            'instant': True,
            'options': options  # 需要配置
        }
        if params:
            self._sheet_data["params"] = params
        if to_file:
            self._sheet_data["to_file"] = to_file
        return self._sheet_data

    def get_new_id(self, task_id, otu_id=None):
        if otu_id:
            new_id = "{}_{}_{}".format(task_id, otu_id[-4:], random.randint(1, 10000))
        else:
            new_id = "{}_{}_{}".format(task_id, random.randint(1000, 10000), random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id
