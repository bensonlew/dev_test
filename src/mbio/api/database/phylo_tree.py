# -*- coding: utf-8 -*-
# __author__ = 'shenghe'


from biocluster.api.database.base import Base, report_check
# import re
import os
from collections import defaultdict
import json
import datetime
# import gridfs
from bson.son import SON
from bson.objectid import ObjectId
# from biocluster.config import Config
# from mainapp.libs.param_pack import group_detail_sort


class PhyloTree(Base):
    def __init__(self, bind_object):
        super(PhyloTree, self).__init__(bind_object)
        self._project_type = 'meta'
        # self._db_name = Config().MONGODB

    # 专门供metabase使用的导表工具，会导入主表以及相关信息，不可用于接口导表
    # 未完成，不可用
    @report_check
    def add_phylo_tree_info_for_meta(self, otu_id):
        collection = self.db["sg_phylo_tree"]
        params = {}
        main_data = [('project_sn', self.bind_object.sheet.project_sn),
                     ('task_id', self.bind_object.sheet.id),
                     ('otu_id', otu_id),
                     ('name', 'tree_origin'),
                     ('status', 'end'),
                     ('params', json.dumps(params, sort_keys=True, separators=(',', ':'))),
                     ('created_ts', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                     ('newicktree', open(self.bind_object.output_dir + '/phylo_tree.tre').read())]
        self.main_id = collection.insert_one(SON(main_data)).inserted_id
        if self.bind_object.sheet.option('color_level_id'):
            self._add_species_categories()
        self.specimen_categories = self._add_format_otu()
        collection.update_one({'_id': self.main_id}, {'$set': {'specimen_categories': self.specimen_categories}})
        self.bind_object.logger.info('Phylo tree导入数据库完成。')
        return str(self.main_id)

    # 专门用于即时计算导表的模块，不可放在metabase中使用。对应的worflow为metabase.report.plot_tree
    @report_check
    def add_phylo_tree_info(self, main_id):
        self.main_id = ObjectId(main_id)
        tree = open(self.bind_object.output_dir + '/phylo_tree.tre')
        if self.bind_object.sheet.option('color_level_id'):
            self._add_species_categories()
        self.specimen_categories = self._add_format_otu()
        main_collection = self.db['sg_phylo_tree']
        main_collection.update_one({'_id': self.main_id},
                                   {'$set': {
                                       'newicktree': tree.read(),
                                       'specimen_categories': self.specimen_categories}})
        tree.close()
        self.bind_object.logger.info('Phylo tree导入数据库完成。')
        return str(self.main_id)

    @report_check
    def _add_species_categories(self):
        species_group = self.bind_object.output_dir + '/species_group.xls'
        if not os.path.isfile(species_group):
            self.categories = None
            return self.categories
        collection = self.db['sg_phylo_tree_species_categories']
        with open(species_group) as f:
            f.readline()
            group = defaultdict(list)
            for i in f:
                line_sp = i.strip().split('\t')
                group[line_sp[1]].append(line_sp[0])
            insert_data = [('phylo_tree_id', self.main_id), ('categories', group.keys()), ('species', group.values())]
            collection.insert_one(SON(insert_data))
            self.categories = group.keys()
            return self.categories

    @report_check
    def _add_format_otu(self):
        collection = self.db['sg_phylo_tree_species_detail']
        with open(self.bind_object.output_dir + '/species_table.xls') as f:
            categories = f.readline().rstrip().split('\t')[1:]
            categories.insert(0, 'species_name')
            insert_data = []
            for i in f:
                one = zip(categories, i.strip().split('\t'))
                one.insert(0, ('phylo_tree_id', self.main_id))
                insert_data.append(SON(one))
        collection.insert_many(insert_data)
        return categories[1:]
