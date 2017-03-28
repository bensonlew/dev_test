# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'

from meta_controller import MetaController
import web

class RefRnaController(MetaController):
    def __init__(self, instant=False):
        super(RefRnaController, self).__init__(instant)
        self.mongodb = Config().MONGODB + '_ref_rna'

    def _update_status_api(self):
        """
        根据client决定接口api为denovo.update_status/denovo.tupdate_status
        """
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if client == 'client01':
            return 'ref-rna.update_status'
        else:
            return 'ref-rna.tupdate_status'
