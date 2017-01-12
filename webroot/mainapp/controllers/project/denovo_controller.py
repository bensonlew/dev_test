# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

from meta_controller import MetaController


class DenovoController(MetaController):
    def __init__(self, instant=False):
        super(DenovoController, self).__init__(instant)

    def _update_status_api(self):
        """
        根据client决定接口api为denovo.update_status/denovo.tupdate_status
        """
        data = web.input()
        client = data.client if hasattr(data, "client") else web.ctx.env.get('HTTP_CLIENT')
        if client == 'client01':
            return 'denovo.update_status'
        else:
            return 'denovo.tupdate_status'
