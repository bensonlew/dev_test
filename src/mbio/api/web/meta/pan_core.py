# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from mbio.api.web.meta import Otu


class PanCore(Otu):

    def __init__(self, data):
        super(PanCore, self).__init__(data)
        self._collection_name = "sg_otu_pan_core"
