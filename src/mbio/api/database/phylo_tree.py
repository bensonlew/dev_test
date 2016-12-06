# -*- coding: utf-8 -*-
# __author__ = 'shenghe'


from biocluster.api.database.base import Base, report_check
# import re
import os
from collections import defaultdict
# import json
import datetime
# import gridfs
from bson.son import SON
from bson.objectid import ObjectId
from biocluster.config import Config
# from mainapp.libs.param_pack import group_detail_sort


class PhyloTree(Base):
    def __init__(self, bind_object):
        super(PhyloTree, self).__init__(bind_object)
        self._db_name = Config().MONGODB

    @report_check
    def add_phylo_tree_info(self):  # 专门用于即时计算导表的模块，不可放在metabase中使用。对应的worflow为metabase.report.plot_tree
        collection = self.db["sg_phylo_tree"]
        main_data = [('project_sn', self.bind_object.sheet.project_sn),
                     ('task_id', self.bind_object.id),
                     ('otu_id', ObjectId(self.bind_object.sheet.option('otu_id'))),
                     ('name', 'tree_{}'.format(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))),
                     ('status', 'end'),
                     ('params', self.bind_object.sheet.option('params')),
                     ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                     ('newicktree', open(self.bind_object.output_dir + '/phylo_tree.tre').read())]
        self.main_id = collection.insert_one(SON(main_data)).inserted_id
        if self.bind_object.sheet.option('color_level_id'):
            self._add_species_categries()
        self.specimen_categries = self._add_format_otu()
        collection.update_one({'_id': self.main_id}, {'$set': {'specimen_categries': self.specimen_categries}})
        self.bind_object.logger.info('Phylo tree导入数据库完成。')
        return str(self.main_id)

    def _add_species_categries(self):
        species_group = self.bind_object.work_dir + '/species_group.xls'
        if not os.path.isfile(species_group):
            self.categries = None
            return self.categries
        collection = self.db['sg_phylo_tree_species_catergries']
        with open(species_group) as f:
            f.readline()
            group = defaultdict(list)
            for i in f:
                line_sp = i.strip().split('\t')
                group[line_sp[1]].append(line_sp[0])
            insert_data = [('phylo_tree_id', self.main_id), ('categries', group.keys()), ('species', group.values())]
            collection.insert_one(SON(insert_data))
            self.categries = group.keys()
            return self.categries

    def _add_format_otu(self):
        collection = self.db['sg_phylo_tree_species_detail']
        with open(self.bind_object.output_dir + '/species_table.xls') as f:
            categries = f.readline().rstrip().split('\t')[1:]
            categries.insert(0, 'species_name')
            insert_data = []
            for i in f:
                one = zip(categries, i.strip().split('\t'))
                one.insert(0, ('phylo_tree_id', self.main_id))
                insert_data.append(SON(one))
        collection.insert_many(insert_data)
        return categries[1:]
