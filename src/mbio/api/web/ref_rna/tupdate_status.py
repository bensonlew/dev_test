# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
from biocluster.config import Config
from mbio.api.web.meta.update_status import UpdateStatus


class TupdateStatus(UpdateStatus):

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._config = Config()
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        self._url = "http://api.tsanger.com/task/add_file"
        self._post_data = "%s&%s" % (self.get_sig(), self.get_post_data())
        self._mongo_client = self._config.mongo_client
        self.mongodb = self._mongo_client[Config().MONGODB + '_ref_rna']