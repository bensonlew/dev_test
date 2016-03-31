# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
# import os
import json
import datetime
import re
from biocluster.api.database.base import Base, report_check
# from bson.objectid import ObjectId
# import re
# import datetime
# from bson.son import SON
# from types import StringTypes


class Anosim(Base):
    def __init__(self, bind_object):
        super(Anosim, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_beta_anosim_result(self, dir_path, main_id=None, main=False, group_id=None,
                               task_id=None, otu_id=None, name=None, params=None, level=9):
        def insert_table_detail(file_path, table_type, update_id,
                                coll_name='sg_beta_multi_anosim_detail',
                                main_coll='sg_beta_multi_anosim',
                                update_column=True, db=self.db, comment='', stats=False,
                                columns=None):
            collection = db[coll_name]
            with open(file_path, 'rb') as f:
                all_lines = f.readlines()
                if comment:
                    flag = 0
                    for line in all_lines:
                        if len(line) > 0:
                            if line[0] == '#':
                                flag += 1
                            else:
                                break
                        else:
                            raise Exception('表中存在空的行')
                    all_lines = all_lines[flag:]
                if isinstance(columns, list):
                    pass
                else:
                    columns = all_lines[0].rstrip().split('\t')[1:]
                for line in all_lines[1:]:
                    values = line.rstrip().split('\t')

                    insert_data = {
                        'anosim_id': update_id,
                        'type': table_type
                        # 'name': values[0]
                    }
                    if stats:
                        insert_data['group1'] = values[0]
                        for m, n in enumerate(values):
                            if re.match(r'^\[[ \.0-9]+\]$', n):
                                values[m] = n.strip('[ ]')
                    else:
                        insert_data['name'] = values[0]
                    values_dict = dict(zip(columns, values[1:]))
                    insert_data = dict(insert_data, **values_dict)
                    collection.insert_one(insert_data)
                if update_column:
                    main_collection = db[main_coll]
                    default_column = {'specimen': 'detail_column', 'factor': 'factor_column', 'vector': 'vector_column',
                                      'species': 'species_column', 'rotation': 'rotation_column', 'box': 'box_column',
                                      'anosim': 'anosim_column', 'stats': 'stats_column'}
                    if table_type in default_column:
                        main_collection.update_one({'_id': update_id},
                                                   {'$set': {default_column[table_type]: ','.join(columns)}},
                                                   upsert=False)

        def insert_text_detail(file_path, data_type, main_id,
                               coll_name='sg_beta_multi_anosim_json_detail', db=self.db):
            collection = db[coll_name]
            with open(file_path, 'rb') as f:
                data = f.read()
                insert_data = {
                    'multi_analysis_id': main_id,
                    'type': data_type,
                    'json_value': data
                }
                collection.insert_one(insert_data)
        if level and level not in range(1, 10):
            raise Exception("level参数%s为不在允许范围内!" % level)
        if task_id is None:
            task_id = self.bind_object.sheet.id
        _main_collection = self.db['sg_beta_multi_anosim']
        if main:
            insert_mongo_json = {
                'project_sn': self.bind_object.sheet.project_sn,
                'task_id': task_id,
                'otu_id': otu_id,
                'level_id': int(level),
                'name': name if name else 'origin',
                'group_id': group_id,
                'params': (json.dumps(params, sort_keys=True, separators=(',', ':'))
                           if isinstance(params, dict) else params),
                'status': 'end',
                'desc': '',
                'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            anosim_id = _main_collection.insert_one(insert_mongo_json).inserted_id
            main_id = anosim_id
        else:
            if not main_id:
                raise Exception('不写入主表时，需要提供主表ID')
        result = _main_collection.find_one({'_id': main_id})
        if result:
            anosim_path = dir_path.rstrip('/') + '/Anosim/format_results.xls'
            box_path = dir_path.rstrip('/') + '/Box/Distances.xls'
            stats_path = dir_path.rstrip('/') + '/Box/Stats.xls'
            insert_table_detail(anosim_path, 'anosim', update_id=main_id, update_column=False,
                                columns=['statisic', 'pvalue', 'permutation_number'])
            insert_table_detail(box_path, 'box', update_id=main_id, update_column=False)
            insert_table_detail(stats_path, 'stats', update_id=main_id, comment='#', stats=True,
                                update_column=False, columns=['group2', 't_statistic',
                                                              'param_pvalue', 'param_correct_pvalue',
                                                              'nonparam_pvalue', 'nonparam_correct_pvalue'])
        else:
            raise Exception('提供的_id：%s在sg_beta_multi_anosim中无法找到表' % str(main_id))
