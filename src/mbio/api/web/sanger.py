# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.wpm.log import Log


class Sanger(Log):

    def __init__(self, data):
        super(Sanger, self).__init__(data)
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://www.sanger.com/api/add_task_log"
        self._post_data = "%s&%s" % (self.get_sig(), self.post_data)
