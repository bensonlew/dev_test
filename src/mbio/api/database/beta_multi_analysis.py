# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import os
from biocluster.api.database.base import Base, report_check
# from bson.objectid import ObjectId
# import re
# import datetime
# from bson.son import SON
# from types import StringTypes


class BetaMultiAnalysis(Base):
    def __init__(self, bind_object):
        super(BetaMultiAnalysis, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_beta_multi_analysis_result(self, dir_path, analysis, main_id):
        def insert_table_detail(file_path, table_type, update_id,
                                coll_name='sg_beta_multi_analysis_detail',
                                main_coll='sg_beta_multi_analysis',
                                update_column=True, db=self.db):
            collection = db[coll_name]
            with open(file_path, 'rb') as f:
                all_lines = f.readlines()
                columns = all_lines[0].rstrip().split('\t')[1:]
                for line in all_lines[1:]:
                    values = line.rstrip().split('\t')
                    insert_data = {
                        'multi_analysis_id': update_id,
                        'type': table_type,
                        'name': values[0]
                    }
                    values_dict = dict(zip(columns, values[1:]))
                    insert_data = dict(insert_data, **values_dict)
                    collection.insert_one(insert_data)
                if update_column:
                    main_collection = db[main_coll]
                    if table_type == 'specimen':
                        main_collection.update_one({'_id': update_id},
                                                   {'$set': {'detail_column': ','.join(columns)}}, upsert=False)
                    else:
                        main_collection.update_one({'_id': update_id},
                                                   {'$set': {'other_column': ','.join(columns)}}, upsert=False)

        def insert_text_detail(file_path, data_type, main_id,
                               coll_name='sg_beta_multi_analysis_json_detail', db=self.db):
            collection = db[coll_name]
            with open(file_path, 'rb') as f:
                data = f.read()
                insert_data = {
                    'multi_analysis_id': main_id,
                    'type': data_type,
                    'json_value': data
                }
                collection.insert_one(insert_data)
        _main_collection = self.db['sg_beta_multi_analysis']
        result = _main_collection.find_one({'_id': main_id})
        if result:
            if analysis == 'pca':
                site_path = dir_path.rstrip('/') + '/Pca/pca_sites.xls'
                rotation_path = dir_path.rstrip('/') + '/Pca/pca_rotation.xls'
                importance_path = dir_path.rstrip('/') + '/Pca/pca_importance.xls'
                insert_table_detail(site_path, 'specimen', update_id=main_id)
                insert_text_detail(rotation_path, 'rotation', main_id=main_id)
                insert_text_detail(importance_path, 'importance', main_id=main_id)
                if result['env_id']:
                    env_path = dir_path.rstrip('/') + '/Pca/pca_envfit_score.xls'
                    env_pr_path = dir_path.rstrip('/') + '/Pca/pca_envfit.xls'
                    insert_table_detail(env_path, 'env', update_id=main_id)
                    insert_text_detail(env_pr_path, 'envfit', main_id=main_id)
                else:
                    pass
            elif analysis == 'pcoa':
                site_path = dir_path.rstrip('/') + '/Pcoa/pcoa_sites.xls'
                insert_table_detail(site_path, 'specimen', update_id=main_id)
                rotation_path = dir_path.rstrip('/') + '/Pcoa/pcoa_rotation.xls'
                importance_path = dir_path.rstrip('/') + '/Pcoa/pcoa_importance.xls'
                insert_text_detail(rotation_path, 'rotation', main_id=main_id)
                insert_text_detail(importance_path, 'importance', main_id=main_id)
            elif analysis == 'nmds':
                site_path = dir_path.rstrip('/') + '/Nmds/nmds_sites.xls'
                insert_table_detail(site_path, 'specimen', update_id=main_id)
            elif analysis == 'dbrda':
                site_path = dir_path.rstrip('/') + '/Dbrda/db_rda_sites.xls'
                factor_path = dir_path.rstrip('/') + '/Dbrda/db_rda_factor.xls'
                insert_table_detail(site_path, 'specimen', update_id=main_id)
                insert_table_detail(factor_path, 'factor', update_id=main_id)
            elif analysis == 'rda_cca':
                if 'rda' in os.listdir(dir_path.rstrip('/') + '/Rda/')[1]:
                    rda_cca = 'rda'
                else:
                    rda_cca = 'cca'
                site_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_sites.xls'
                species_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_species.xls'
                importance_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_importance.xls'
                environment_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_environment.xls'
                dca_path = dir_path.rstrip('/') + '/Rda/' + 'dca.txt'
                insert_table_detail(site_path, 'specimen', update_id=main_id)
                insert_table_detail(species_path, 'species', update_id=main_id)
                insert_table_detail(environment_path, 'env', update_id=main_id)
                insert_text_detail(importance_path, 'importance', main_id=main_id)
                insert_text_detail(dca_path, 'dca', main_id=main_id)
            else:
                raise Exception('提供的analysis：%s不存在' % analysis)
        else:
            raise Exception('提供的_id：%s在sg_beta_multi_analysis中无法找到表' % str(main_id))
