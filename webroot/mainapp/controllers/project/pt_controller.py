# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'

from meta_controller import MetaController
import web

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
            return 'pt.update_status'
        else:
            return 'pt.tupdate_status'

    @check_sig
    def POST(self):
        workflow_client = Basic(data=self.sheet_data, instant=self.instant)
        try:
            run_info = workflow_client.run()
            self._return_msg = workflow_client.return_msg
            return run_info
        except Exception, e:
            return {"success": False, "info": "运行出错: %s" % e}
