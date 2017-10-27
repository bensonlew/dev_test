# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'

from meta_controller import MetaController
import web
from mainapp.models.mongo.submit.paternity_test_mongo import PaternityTest
from ..core.basic import Basic
from mainapp.libs.signature import check_sig
import json
import random


class PtController(MetaController):
    def __init__(self, instant=False):
        super(PtController, self).__init__(instant)

    def _update_status_api(self):
        """
        根据client决定接口api为denovo.update_status/denovo.tupdate_status
        """
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if client == 'client01':
            return 'pt.med_report_update'
        else:
            return 'pt.med_report_tupdate'

    # def set_sheet_data(self, *arg, **kwarg):
    #     print arg, kwarg
    #     super(PtController, self).set_sheet_data(*arg, **kwarg)
    @check_sig
    def POST(self):
        workflow_client = Basic(data=self.sheet_data, instant=self.instant)
        try:
            run_info = workflow_client.run()
            self._return_msg = workflow_client.return_msg
            return run_info
        except Exception, e:
            return json.dumps({"success": False, "info": "运行出错：{}".format(e)})

    def set_sheet_data(self, name, options, module_type="workflow", params=None, to_file=None):
        self._post_data = web.input()
        task_info = PaternityTest().get_query_info(self.data.father_id)
        # project_sn = task_info['project_sn']
        new_task_id = self.get_new_id(self.data.father_id)+"_111"
        self._sheet_data = {
            'id': new_task_id,
            'stage_id': 0,
            'name': name,
            'type': module_type,
            'interaction': True,
            'client': self.data.client,
            # 'project_sn': project_sn,
            'IMPORT_REPORT_DATA': True,
            'UPDATE_STATUS_API': self._update_status_api(),
            'instant': False,
            'options': options
        }
        if self.instant:
            self._sheet_data['instant'] = True
        if params:
            self._sheet_data['params'] = params
        return self._sheet_data

    def set_sheet_data_(self, name, options, module_type="workflow", params=None, to_file=None):
        self._post_data = web.input()
        new_id = 'pt_{}_{}'.format(random.randint(1000, 10000), random.randint(1, 10000))
        print new_id
        self._sheet_data = {
            'id': new_id,
            'name': name,
            'type': module_type,
            'client': self.data.client,
            'IMPORT_REPORT_DATA': True,
            'UPDATE_STATUS_API': self._update_status_api(),
            'instant': False,
            'options': options
        }
        print self._sheet_data
        if self.instant:
            self._sheet_data['instant'] = True
        if params:
            self._sheet_data['params'] = params
        return self._sheet_data
