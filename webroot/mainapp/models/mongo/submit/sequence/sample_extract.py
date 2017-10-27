# -*- coding: utf-8 -*-
# __author__ = 'xuting'

from mainapp.models.mongo.core.base import Base


# from mainapp.config.db import get_mongo_client
# from biocluster.config import Config


class SampleExtract(Base):
    def __init__(self, bind_object=None):
        super(SampleExtract, self).__init__(bind_object)
        self._project_type = 'meta'
        # self.client = get_mongo_client()
        # self.db = self.client[Config().MONGODB]

    def add_sg_seq_sample(self, task_id, file_path, params, query_id):
        insert_data = {
            "task_id": task_id,
            "file_path": file_path,
            "params": params,
            "query_id": query_id
        }
        collection = self.db["sg_seq_sample"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id
