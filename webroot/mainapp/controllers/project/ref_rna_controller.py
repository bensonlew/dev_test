# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import random
from ..core.basic import Basic
from mainapp.libs.signature import check_sig
from mainapp.models.mongo.meta import Meta
from mainapp.models.mongo.ref_rna import RefRna
from meta_controller import MetaController
from biocluster.config import Config


class RefRnaController(MetaController):
    def __init__(self, instant=False):
        super(RefRnaController, self).__init__(instant)
        self.mongodb = Config().MONGODB + '_ref_rna'
        self.ref_rna = Meta(self.mongodb)

    def _update_status_api(self):
        """
        根据client决定接口api为ref_rna.update_status/ref_rna.tupdate_status
        """
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if client == 'client01':
            return 'ref_rna.update_status'
        else:
            return 'ref_rna.tupdate_status'

    def set_sheet_data(self, name, options, main_table_name, module_type="workflow", params=None, to_file=None):
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
        if hasattr(self._post_data, "geneset_id"):
            table_info = RefRna().get_main_info(self._post_data["geneset_id"].split(",")[0], "sg_geneset")
        if hasattr(self._post_data, "express_id"):
            table_info = RefRna().get_main_info(self._post_data["express_id"], "sg_express")
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
            'UPDATE_STATUS_API': self._update_status_api(),
            'options': options  # 需要配置
        }
        if self.instant:
            self._sheet_data["instant"] = True
        if params:
            self._sheet_data["params"] = params
        if to_file:
            self._sheet_data["to_file"] = to_file
        # if main_table_name:
        #     self._sheet_data["main_table_name"] = main_table_name
        print('Sheet_Data: {}'.format(self._sheet_data))
        self.workflow_id = new_task_id
        # self.meta_pipe()
        return self._sheet_data
