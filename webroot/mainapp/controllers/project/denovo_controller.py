# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import random
from ..core.basic import Basic
from mainapp.libs.signature import check_sig
from mainapp.models.mongo.meta import Meta
from mainapp.models.workflow import Workflow
from meta_controller import MetaController
from biocluster.config import Config


class DenovoController(MetaController):
    def __init__(self, instant=False):
        super(DenovoController, self).__init__(instant)
        self.mongodb = Config().MONGODB + '_rna'
        self.denovo = Meta(self.mongodb)

    def _update_status_api(self):
        """
        根据client决定接口api为denovo.update_status/denovo.tupdate_status
        """
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if client == 'client01':
            return 'denovo.denovo_update_status'
        else:
            return 'denovo.tupdate_status'

    @check_sig
    def POST(self):
        workflow_client = Basic(data=self.sheet_data, instant=self.instant)
        try:
            run_info = workflow_client.run()
            self._return_msg = workflow_client.return_msg
            return run_info
        except Exception, e:
            return {"success": False, "info": "运行出错: %s" % e }
