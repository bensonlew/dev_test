# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
from biocluster.config import Config
from mbio.api.web.pt.med_report_tupdate import MedReportTupdate


class MedReportUpdate(MedReportTupdate):

    def __init__(self, data):
        super(MedReportUpdate, self).__init__(data)
        self._config = Config()
        self._client = "client01"
        self._key = "1ZYw71APsQ"
        self._url = "http://www.sanger.com/api/add_file"
        # self._post_data = "%s&%s" % (self.get_sig(), self.get_post_data())
        self._mongo_client = self._config.mongo_client
        self.mongodb = self._mongo_client[Config().MONGODB + '_nipt']
