# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.config import Config
from ..meta.update_status import UpdateStatus


class DenovoUpdateStatus(UpdateStatus):

    def __init__(self, data):
        super(DenovoUpdateStatus, self).__init__(data)
        self._config = Config()
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://www.sanger.com/api/add_file"
        self.mongodb = self._mongo_client[Config().MONGODB + '_rna']
