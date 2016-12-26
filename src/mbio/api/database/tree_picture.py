# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.api.database.base import Base, report_check
import re
import json
import datetime
import gridfs
from bson.son import SON
from bson.objectid import ObjectId
from biocluster.config import Config
from mainapp.libs.param_pack import group_detail_sort


class TreePicture(Base):
    def __init__(self, bind_object):
        super(TreePicture, self).__init__(bind_object)
        self._db_name = Config().MONGODB

    @report_check
    def add_tree_picture(self, output_dir, major=False, params=None, main_id=None, otu_id=None, level=9, name=None):
        task_id = self.bind_object.id
        if major:
            if not otu_id:
                raise Exception("写主表时需要otu_id")
            else:
                if not isinstance(otu_id, ObjectId):
                    otu_id = ObjectId(otu_id)
            collection = self.db["sg_otu"]
            result = collection.find_one({"_id": otu_id})
            task_id = result['task_id']
            collection = self.db["sg_tree_picture"]
            insert_data = {
                "project_sn": self.bind_object.sheet.project_sn,
                "task_id": task_id,
                "otu_id": otu_id,
                "level_id": level,
                "name": name if name else "plot_tree_origin",
                "status": "end",
                "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            main_id = collection.insert_one(insert_data).inserted_id
        else:
            pass
        if main_id is None:
            raise Exception("major为False时需提供main_id!")
        if not isinstance(main_id, ObjectId):
            main_id = ObjectId(main_id)
        fs = gridfs.GridFS(self.db)
        # fs = gridfs.GridFS(self.db['sg_tree_picture_file'])
        fan_tree_id = fs.put(open(output_dir + '/fan.png', 'r'))
        bar_tree_id = fs.put(open(output_dir + '/bar.png', 'r'))
        update_data = {
            'bar_tree': bar_tree_id,
            'fan_tree': fan_tree_id
        }
        collection = self.db["sg_tree_picture"]
        collection.update_one({'_id': main_id}, {'$set': update_data})
        return main_id
