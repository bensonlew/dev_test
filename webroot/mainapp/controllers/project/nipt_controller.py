# -*- coding: utf-8 -*-
# __author__ = 'hongdong.xuan'

from meta_controller import MetaController
import web
from ..core.basic import Basic
from mainapp.libs.signature import check_sig
import json
import random


class NiptController(MetaController):
    def __init__(self, instant=False):
        super(NiptController, self).__init__(instant)

    def _update_status_api(self):
        """
        根据client决定接口api为denovo.update_status/denovo.tupdate_status
        """
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if client == 'client01':
            return 'nipt.med_report_tupdate'
        else:
            return 'nipt.med_report_update'

    @check_sig
    def POST(self):
        workflow_client = Basic(data=self.sheet_data, instant=self.instant)
        try:
            run_info = workflow_client.run()
            self._return_msg = workflow_client.return_msg
            return run_info
        except Exception, e:
            return json.dumps({"success": False, "info": "运行出错：{}".format(e)})

    def set_sheet_data_(self, name, options, module_type="workflow", params=None, to_file=None):
        self._post_data = web.input()
        new_id = 'nipt_{}_{}'.format(random.randint(1000, 10000), random.randint(1, 10000))
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
