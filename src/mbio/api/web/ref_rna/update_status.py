# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
from biocluster.config import Config
from ..meta.update_status import UpdateStatus as Us


class UpdateStatus(Us):

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._config = Config()
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://api.sanger.com/task/add_file"
        self.mongodb = self._mongo_client[Config().MONGODB + '_ref_rna']
