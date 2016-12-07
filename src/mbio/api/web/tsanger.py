# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.wpm.log import Log


class Tsanger(Log):

    def __init__(self, data):
        super(Tsanger, self).__init__(data)
        self._client = "client03"
        self._key = "hM4uZcGs9d"
        self._url = "http://www.tsanger.com/api/add_task_log"
        self._post_data = "%s&%s" % (self.get_sig(), self.post_data)

