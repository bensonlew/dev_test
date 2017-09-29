# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'

from biocluster.api.database.base import Base, report_check
import datetime
from bson.objectid import ObjectId
from types import StringTypes
from biocluster.config import Config


class Composition(Base):
    def __init__(self, bind_object):
        super(Composition, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_composition(self, dir_path, type, geneset_id, anno_id, level_id, specimen_group, species_tree=None,
                        specimen_tree=None,
                        name=None, params=None, spname_spid=None, specimen_list=None, species_list=None):
        if specimen_tree:
            specimen_tree_path = dir_path.rstrip('/') + '/heatmap/specimen_hcluster.tre'
            with open(specimen_tree_path, 'rb') as f:
                specimen_tree = f.readlines()
        if species_tree:
            species_tree_path = dir_path.rstrip('/') + '/heatmap/species_hcluster.tre'
            with open(species_tree_path, 'rb') as f:
                species_tree = f.readlines()
        task_id = self.bind_object.sheet.id
        if spname_spid:
            group_detail = {'All': [str(i) for i in spname_spid.values()]}
            params['group_detail'] = group_detail_sort(group_detail)
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": task_id,
            "geneset_id": geneset_id,
            "anno_id": anno_id,
            "name": name,
            "level_id": level_id,
            "status": "end",
            "specimen_tree": specimen_tree if specimen_tree else "()",
            "species_tree": species_tree if species_tree else "()",
            "species_list": species_list if species_list else "[]",
            "specimen_list": specimen_list if specimen_list else "[]",
            "specimen_group": specimen_group,
            "type": type,
            "desc": "",
            "params": "",
            # "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["composition"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        table_path = ""
        if type == "bar":
            table_path = dir_path.rstrip('/') + '/bar/taxa.percents.table.xls'
        if type == "circos":
            table_path = dir_path.rstrip('/') + '/circos/taxa.percents.table.xls'
        if type == "heatmap":
            table_path = dir_path.rstrip('/') + '/heatmap/taxa.percents.table.xls'
        self.add_composition_detail(table_path, composition_id=inserted_id)
        return inserted_id

    @report_check
    def add_composition_detail(self, file_path, composition_id=None):
        data_list = []
        with open(file_path, "r") as f:
            envs = f.readline().strip().split("\t")[1:]
            for line in f:
                line = line.strip().split("\t")
                data = {
                    "composition_id": ObjectId(composition_id),
                    "species_name": line[0],
                }
                for n, e in enumerate(envs):
                    data[e] = line[n + 1]
                data_list.append(data)
        try:
            collection = self.db["composition_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.info("导入丰度表格出错:%s" % e)
        else:
            self.bind_object.logger.info("导入丰度表格成功")
