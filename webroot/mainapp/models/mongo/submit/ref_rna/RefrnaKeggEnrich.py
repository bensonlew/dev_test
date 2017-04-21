# -*- coding: utf-8 -*-
# __author__ = 'shijin'

import datetime
from biocluster.config import Config
from ..denovo_rna.denovo_kegg_enrich import DenovoEnrich

class RefrnaKeggEnrich(DenovoEnrich):
    def __init__(self):
        super(DenovoEnrich, self).__init__()
        self.client = Config().mongo_client
        self.db_name = Config().MONGODB + '_ref_rna'
        self.db = self.client[self.db_name]

