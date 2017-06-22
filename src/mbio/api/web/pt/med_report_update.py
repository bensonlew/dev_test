# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
from .med_report_tupdate import MedReportTupdate


class MedReportUpdate(MedReportTupdate):

    def __init__(self, data):
        super(MedReportUpdate, self).__init__(data)
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://api.sanger.com/task/add_file"
        self.mongodb = self._mongo_client[self.config.MONGODB+'_paternity_test']
