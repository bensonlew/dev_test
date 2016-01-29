# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.api.database.base import Base, report_check
import os
import datetime
from bson.objectid import ObjectId
from types import StringTypes
from mainapp.config.db import get_mongo_client


class Rarefaction(Base):
    def __init__(self, bind_object):
        super(Rarefaction, self).__init__(bind_object)
        self._db_name = "sanger"
        self.client = get_mongo_client()
        self.category_x = None
        self.category_y = None

    @report_check
    def add_rarefaction_detail(self, rare_id, file_path):
        if not isinstance(rare_id, ObjectId):
            if isinstance(rare_id, StringTypes):
                rare_id = ObjectId(rare_id)
            else:
                raise Exception("pan_core_id必须为ObjectId对象或其对应的字符串!")
        rare_paths = os.listdir(file_path)
        rare_detail = []
        for rare_path in rare_paths:
            # print os.path.join(file_path,rare_path)
            files = os.listdir(rare_path)
            # print files
            fs_path = []
            for f in files:
                fs_path.append(os.path.join(file_path, rare_path, f))
            for fs in fs_path:
                rarefaction = []
                sample_name = fs.split('.')[1]
                with open(fs) as f:
                    while True:
                        line = f.readline().strip('\n')
                        if not line:
                            break
                        line_data = line.split("\t")
                        # print line_data
                        my_dic = dict()
                        my_dic["column"] = line_data[0]
                        my_dic["value"] = line_data[1]
                        rarefaction.append(my_dic)
                    rarefaction.pop(0)
                    # print rarefaction
                    insert_data = {
                        "rare_id": rare_id,
                        "index_type": rare_path,
                        "specimen_name": sample_name,
                        "json_value": rarefaction
                    }
                    print insert_data
                    rare_detail.append(insert_data)
        try:
            collection = self.db['sg_alpha_rarefaction_curve_detail']
            collection.insert_many(rare_detail)
        except Exception as e:
            self.bind_object.logger.error("导入rare_detail表格{}信息出错:{}".format(file_path, e))
        else:
            self.bind_object.logger.info("导入rare_detail表格{}成功".format(file_path))

    @report_check
    def add_rare_table(self, file_path, level, otu_id=None, task_id=None, name=None, params=None):
        if level not in range(1, 10):
            raise Exception("level参数%s为不在允许范围内!" % level)
        if task_id is None:
            task_id = self.bind_object.sheet.id
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": task_id,
            "otu_id": otu_id,
            "name": name if name else "rarefaction_origin",
            "level_id": level,
            "status": "end",
            "params": params,
            "category_x": self.category_x,
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        if params is not None:
            insert_data['params'] = params
        collection = self.db["sg_alpha_rarefaction_curve"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        self.add_rarefaction_detail(inserted_id, file_path)
