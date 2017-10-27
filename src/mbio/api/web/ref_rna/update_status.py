# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
from ..meta.update_status import UpdateStatus as Us


class UpdateStatus(Us):

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self.mongodb = self._mongo_client[self.config.get_mongo_dbname(ref_rna)]
