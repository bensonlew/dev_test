# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from biocluster.api.web.log import Log
import random
import hashlib
import urllib
import time


class Sanger(Log):

    def __init__(self, data):
        super(Sanger, self).__init__(data)
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://172.16.3.74/api/add_task_log"
        self._post_data = "%s&%s" % (self.get_sig(), self.data.data)

    def get_sig(self):
        nonce = str(random.randint(1000, 10000))
        timestamp = str(int(time.time()))
        x_list = [self._key, timestamp, nonce]
        x_list.sort()
        sha1 = hashlib.sha1()
        map(sha1.update, x_list)
        sig = sha1.hexdigest()
        signature = {
            "client": self._client,
            "nonce": nonce,
            "timestamp": timestamp,
            "signature": sig
        }
        return urllib.urlencode(signature)
