# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.api.web.log import Log
import urllib
import json


class SplitData(Log):

    def __init__(self, data):
        super(SplitData, self).__init__(data)
        # self._client = "client01"
        # self._key = "1ZYw71APsQ"
        self._url = "http://172.16.6.15:8080/api/split/receive_pipeline"
        # self._url = "http://172.16.6.96/html/code.php"
        parsered_data = self.parse_data(self.post_data)
        self._post_data = "%s" % parsered_data

    def parse_data(self, post_data):
        my_content = post_data["content"]
        try:
            my_content = json.loads(my_content)
        except Exception:
            pass
        post_data["content"] = json.dumps(my_content)
        return urllib.urlencode(post_data)

    @property
    def post_data(self):
        data = json.loads(self.data.data)
        return data
