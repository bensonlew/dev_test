# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import json
import urllib
from biocluster.wpm.log import Log
from biocluster.core.function import CJsonEncoder, filter_error_info


class Tsanger(Log):

    def __init__(self, data):
        super(Tsanger, self).__init__(data)
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        self._url = "http://api.tsg.com/task/add_task_log"
        self._post_data = "%s&%s" % (self.get_sig(), self.post_data)

    def update(self):
        self.send()

    # @property
    # def post_data(self):
    #     my_content = self.data["content"]
    #     if 'stage' in my_content:
    #         my_content['stage']['error'] = filter_error_info(
    #             my_content['stage']['error'])
    #     my_data = dict()
    #     if 'files' in my_content:
    #         files = my_content.pop('files')
    #         if 'stage' in my_content:
    #             my_content['stage']['files'] = files
    #     if 'dirs' in my_content:
    #         dirs = my_content.pop('dirs')
    #         if 'stage' in my_content:
    #             my_content['stage']['dirs'] = dirs
    #     my_data["content"] = json.dumps(my_content, cls=CJsonEncoder)
    #     return urllib.urlencode(my_data)
