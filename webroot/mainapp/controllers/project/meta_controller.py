# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import web
import random
from ..core.basic import Basic
from mainapp.libs.input_check import meta_check
from mainapp.models.mongo.meta import Meta
from mainapp.libs.signature import check_sig
from mainapp.models.mongo.distance_matrix import Distance
from mainapp.models.workflow import Workflow
from biocluster.config import Config


class MetaController(object):

    def __init__(self, instant=False):
        self._instant = instant
        self._post_data = None
        self._sheet_data = None
        self._return_msg = None
        self.mongodb = Config().MONGODB

    @property
    def data(self):
        """
        获取Post数据

        :return:
        """
        return self._post_data

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
        workflow_client = Basic(data=self.sheet_data, instant=self.instant)
        try:
            run_info = workflow_client.run()
            self._return_msg = workflow_client.return_msg
            return run_info
        except Exception, e:
            return {"success": False, "info": "运行出错: %s" % e }

    def set_sheet_data(self, name, options, main_table_name, module_type="workflow", params=None, to_file=None, main_id=None, collection_name=None):
        """
        设置运行所需的Json文档

        :param name: workflow/module/tool相对路径
        :param module_type: workflow/module/tool
        :param main_table_name: 交互分析项主表名称
        :param options: workflow/module/tool参数
        :param params: 交互分析主表params字段
        :param to_file: workflow/module/tool mongo数据转文件
        :return:
        """
        self._post_data = web.input()
        # added by qiuping 20170111
        if not main_id:
            main_id = self.data.otu_id
            collection_name = 'sg_otu'
        table_info = Meta(db=self.mongodb).get_main_info(main_id=main_id, collection_name=collection_name)
        # modify end
        project_sn = table_info["project_sn"]
        task_id = table_info["task_id"]
        new_task_id = self.get_new_id(task_id)
        self._sheet_data = {
            'id': new_task_id,
            'stage_id': 0,
            'name': name,  # 需要配置
            'type': module_type,  # 可以配置
            'client': self.data.client,
            'output': self._create_output_dir(task_id, main_table_name),
            'project_sn': project_sn,
            'IMPORT_REPORT_DATA': True,
            'main_table_name': main_table_name,
            'UPDATE_STATUS_API': self._update_status_api(),
            'options': options  # 需要配置
        }
        if self.instant:
            self._sheet_data["instant"] = True
        if params:
            self._sheet_data["params"] = params
        if to_file:
            self._sheet_data["to_file"] = to_file
        if main_table_name:
            self._sheet_data["main_table_name"] = main_table_name
        print('Sheet_Data: {}'.format(self._sheet_data))
        return self._sheet_data

    def get_new_id(self, task_id, otu_id=None):
        """
        根据旧的ID生成新的workflowID，固定为旧的后面用“_”，添加两次随机数或者一次otu_id一次随机数
        """
        if otu_id:
            new_id = "{}_{}_{}".format(task_id, otu_id[-4:], random.randint(1, 10000))
        else:
            new_id = "{}_{}_{}".format(task_id, random.randint(1000, 10000), random.randint(1, 10000))
        workflow_module = Workflow()
        workflow_data = workflow_module.get_by_workflow_id(new_id)
        if len(workflow_data) > 0:
            return self.get_new_id(task_id, otu_id)
        return new_id

    def _create_output_dir(self, task_id, main_table_name):
        """
        根据主表名称，生成结果目录名称/上传路径
        """
        data = web.input()
        # modified by qiuping 20170111
        task_info = Meta(db=self.mongodb).get_task_info(task_id)
        # modified end
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if client == 'client01':
            target_dir = 'sanger'
        else:
            target_dir = 'tsanger'
        target_dir += ':rerewrweset/files/' + str(task_info['member_id']) + \
            '/' + str(task_info['project_sn']) + '/' + \
            task_id + '/report_results/' + main_table_name
        return target_dir

    def _update_status_api(self):
        """
        根据client决定接口api为meta.update_status/meta.tupdate_status
        """
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if client == 'client01':
            return 'meta.update_status'
        else:
            return 'meta.tupdate_status'
