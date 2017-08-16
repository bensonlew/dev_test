# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.config import Config
from .update_status import UpdateStatus


class TupdateStatus(UpdateStatus):

    def __init__(self, data):
        super(TupdateStatus, self).__init__(data)
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        self._url = "http://api.tsanger.com/task/add_file"
        self.mongodb = self._mongo_client[self.config.MONGODB]
