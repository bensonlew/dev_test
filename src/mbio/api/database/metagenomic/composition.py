# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'


from biocluster.api.database.base import Base, report_check
import json
import datetime
from bson.objectid import ObjectId
from biocluster.config import Config


class Composition(Base):
    def __init__(self, bind_object):
        super(Composition, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_composition(self, graphic_type, geneset_id, anno_id, level_id, specimen_group, species_tree=None,
                        specimen_tree=None, name=None, params=None, spname_spid=None, specimen_list=None,
                        species_list=None):
        task_id = self.bind_object.sheet.id
        if spname_spid:
            group_detail = {'All': [str(i) for i in spname_spid.values()]}
            params['group_detail'] = group_detail_sort(group_detail)
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": task_id,
            "geneset_id": geneset_id,
            "anno_id": anno_id,
            "name":  self.bind_object.sheet.main_table_name if self.bind_object.sheet.main_table_name else name,
            "level_id": level_id,
            "status": "end",
            "specimen_tree": specimen_tree,
            "species_tree": species_tree,
            "species_list": species_list,
            "specimen_list": specimen_list,
            "specimen_group": specimen_group,
            "graphic_type": graphic_type,
            "desc": "组成分析",
            "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["composition"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    @report_check
    def add_composition_detail(self, file_path, composition_id, species_tree=None, specimen_tree=None):
        species_list = ""
        specimens_list = ""
        if specimen_tree != "":
            with open(specimen_tree, 'r') as f:
                specimen_tree = f.readline()
        if species_tree != "":
            with open(species_tree, 'r') as f:
                species_tree = f.readline()
        data_list = []
        with open(file_path, "r") as f:
            specimens = f.readline().strip().split("\t")[1:]
            specimens_list = ','.join(specimens)
            for line in f:
                line = line.strip().split("\t")
                data = {
                    "composition_id": ObjectId(composition_id),
                    "species_name": line[0],
                }
                species_list = species_list + "," + line[0]
                species_list = species_list.lstrip(',')
                for n, e in enumerate(specimens):
                    data[e] = line[n + 1]
                data_list.append(data)
        try:
            collection = self.db["composition_detail"]
            collection.insert_many(data_list)
            self.bind_object.logger.info("开始刷新主表写树")
            main_collection = self.db["composition"]
            main_collection.update({"_id": ObjectId(composition_id)}, {"$set": {"specimen_tree": specimen_tree,
                                                                                "species_tree": species_tree,
                                                                                "specimen_list": specimens_list,
                                                                                "species_list": species_list}})
        except Exception, e:
            self.bind_object.logger.info("导入丰度表格出错:%s" % e)
        else:
            self.bind_object.logger.info("导入丰度表格成功")
