# -*- coding: utf-8 -*-
# __author__ = 'wangzhaoyue'

from mainapp.config.db import get_mongo_client
from biocluster.config import Config
import datetime


class SampleExtract(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client["samplebase"]

    def add_sg_seq_sample(self, member_id, type, status):
        insert_data = {
            "member_id": member_id,
            "type": type,
            "status": "start",
            "batch_sn": 'SampleBase_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
        }
        collection = self.db["sg_test_batch"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

