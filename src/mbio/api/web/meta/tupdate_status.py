# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.config import Config
from update_status import UpdateStatus


class TupdateStatus(UpdateStatus):

    def __init__(self, data):
        super(UpdateStatus, self).__init__(data)
        self._config = Config()
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        self._url = "http://www.tsanger.com/api/add_file"
        self.update_info = self.data["content"]["update_info"] if "update_info" in self.data["content"].keys() else None
        self._post_data = "%s&%s" % (self.get_sig(), self.get_post_data())
        self._mongo_client = self._config.mongo_client
        self.mongodb = self._mongo_client[Config().MONGODB]
