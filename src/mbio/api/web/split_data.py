# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.api.web.log import Log


class SplitData(Log):

    def __init__(self, data):
        super(SplitData, self).__init__(data)
        # self._client = "client01"
        # self._key = "1ZYw71APsQ"
        self._url = "http://172.16.3.16/sequen/split_result"
        self._post_data = "%s" % self.post_data
