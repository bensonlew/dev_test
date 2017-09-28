# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'

from biocluster.api.database.base import Base, report_check
import datetime
from bson.objectid import ObjectId
from types import StringTypes
from biocluster.config import Config


class HeatmapCor(Base):
    def __init__(self, bind_object):
        super(HeatmapCor, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_heatmap_cor(self, dir_path, geneset_id, anno_id, level_id, env_id, specimen_group, species_tree=None,
                        env_tree=None,
                        name=None, params=None, spname_spid=None, env_list=None, species_list=None):
        if env_tree:
            env_tree_path = dir_path.rstrip('/') + '/env_tree.tre'
            with open(env_tree_path, 'rb') as f:
                env_tree = f.readlines()
        if species_tree:
            species_tree_path = dir_path.rstrip('/') + '/species_tree.tre'
            with open(species_tree_path, 'rb') as f:
                species_tree = f.readlines()
        task_id = self.bind_object.sheet.id
        if spname_spid:
            group_detail = {'All': [str(i) for i in spname_spid.values()]}
            params['group_detail'] = group_detail_sort(group_detail)
        insert_data = {
            "project_sn": self.bind_object.sheet.project_sn,
            "task_id": task_id,
            "env_id": env_id,
            "geneset_id": geneset_id,
            "anno_id": anno_id,
            "name": name if name else "pearson_origin",
            "level_id": level_id,
            "status": "end",
            "env_tree": env_tree if env_tree else "()",
            "species_tree": species_tree if species_tree else "()",
            "env_list": env_list if env_list else "[]",
            "species_list": species_list if species_list else "[]",
            'specimen_group': specimen_group,
            "desc": "",
            "params": "",
            # "params": json.dumps(params, sort_keys=True, separators=(',', ':')),
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["heatmap_cor"]
        correlation_path = dir_path.rstrip('/') + '/pearsons_correlation.xls'
        pvalue_path = dir_path.rstrip('/') + '/pearsons_pvalue.xls'
        self.add_heatmap_cor_detail(correlation_path, "correlation", heatmap_cor_id=None, species_tree=None,
                                    env_tree=None,
                                    env_list=None, species_list=None)
        self.add_heatmap_cor_detail(pvalue_path, "pvalue", heatmap_cor_id=None, species_tree=None, env_tree=None,
                                    env_list=None, species_list=None)
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    @report_check
    def add_heatmap_cor_detail(self, file_path, value_type, heatmap_cor_id=None):
        data_list = []
        with open(file_path, "r") as f:
            envs = f.readline().strip().split("\t")[1:]
            for line in f:
                line = line.strip().split("\t")
                data = {
                    "heatmap_cor_id": ObjectId(heatmap_cor_id),
                    "species_name": line[0],
                    "type": value_type
                }
                for n, e in enumerate(envs):
                    data[e] = line[n + 1]
                data_list.append(data)
        try:
            collection = self.db["heatmap_cor_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.info("导入相关性热图结果文件出错:%s" % e)
        else:
            self.bind_object.logger.info("导入相关性热图结果文件成功")
