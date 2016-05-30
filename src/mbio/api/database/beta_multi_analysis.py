# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import os
import json
import datetime
import re
from biocluster.api.database.base import Base, report_check
from bson.objectid import ObjectId
from types import StringTypes
# import re
# import datetime
# from bson.son import SON


class BetaMultiAnalysis(Base):
    def __init__(self, bind_object):
        super(BetaMultiAnalysis, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_beta_multi_analysis_result(self, dir_path, analysis, main_id=None, main=False, env_id=None, group_id=None,
                                       task_id=None, otu_id=None, name=None, params=None, level=9, remove=None):
        def insert_table_detail(file_path, table_type, update_id,
                                coll_name='sg_beta_multi_analysis_detail',
                                main_coll='sg_beta_multi_analysis',
                                update_column=True, db=self.db, fileter_biplot=None, remove_key_blank=False):
            collection = db[coll_name]
            with open(file_path, 'rb') as f:
                all_lines = f.readlines()
                columns = all_lines[0].rstrip().split('\t')[1:]
                if remove_key_blank:
                    columns = [i.replace(' ', '') for i in columns]
                for line in all_lines[1:]:
                    values = line.rstrip().split('\t')
                    if fileter_biplot:
                        if not isinstance(fileter_biplot, list):
                            raise Exception('需要删除的匹配列必须为列表')
                        flag = 0
                        for i in fileter_biplot:
                            if re.match(r'^{}'.format(i), values[0]):
                                flag = 1
                        if flag:
                            continue
                    else:
                        pass
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
                    # default_column = {'specimen': 'detail_column', 'factor': 'factor_column',
                    #                   'vector': 'vector_column',
                    #                   'species': 'species_column', 'rotation': 'rotation_column'}
                    default_column = {'specimen': 'detail_column', 'factor': 'factor_column', 'vector': 'vector_column',
                                      'species': 'species_column'}
                    if table_type in default_column:
                        main_collection.update_one({'_id': update_id},
                                                   {'$set': {default_column[table_type]: ','.join(columns)}},
                                                   upsert=False)
                    else:
                        raise Exception('错误的表格类型：%s不能在主表中插入相应表头' % table_type)

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
        if level and level not in range(1, 10):
            raise Exception("level参数%s为不在允许范围内!" % level)
        if task_id is None:
            task_id = self.bind_object.sheet.id
        if not isinstance(env_id, ObjectId) and env_id is not None:
            env_id = ObjectId(env_id)
        if not isinstance(group_id, ObjectId) and group_id is not None:
            group_id = ObjectId(group_id)
        if not isinstance(otu_id, ObjectId) and otu_id is not None:
            otu_id = ObjectId(otu_id)
        _main_collection = self.db['sg_beta_multi_analysis']
        if main:
            if env_id:
                params['env_id'] = str(env_id)    # env_id在再metabase中不可用
            params['otu_id'] = str(otu_id)  # otu_id在再metabase中不可用
            insert_mongo_json = {
                'project_sn': self.bind_object.sheet.project_sn,
                'task_id': task_id,
                'otu_id': otu_id,
                'level_id': int(level),
                'name': analysis + '_' + name if name else 'origin',
                'table_type': analysis,
                'env_id': env_id,
                'group_id': group_id,
                'params': json.dumps(params, sort_keys=True, separators=(',', ':')),
                'status': 'end',
                'desc': '',
                'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            multi_analysis_id = _main_collection.insert_one(insert_mongo_json).inserted_id
            main_id = multi_analysis_id
        else:
            if not main_id:
                raise Exception('不写入主表时，需要提供主表ID')
            if not isinstance(main_id, ObjectId):
                main_id = ObjectId(main_id)
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
                    filelist = os.listdir(dir_path.rstrip('/') + '/Pca')
                    if 'pca_envfit_factor_scores.xls' in filelist:
                        env_fac_path = dir_path.rstrip('/') + '/Pca/pca_envfit_factor_scores.xls'
                        env_fac_pr_path = dir_path.rstrip('/') + '/Pca/pca_envfit_factor.xls'
                        insert_table_detail(env_fac_path, 'factor', update_id=main_id)
                        insert_text_detail(env_fac_pr_path, 'env_factor', main_id=main_id)
                    if 'pca_envfit_vector_scores.xls' in filelist:
                        env_vec_path = dir_path.rstrip('/') + '/Pca/pca_envfit_vector_scores.xls'
                        env_vec_pr_path = dir_path.rstrip('/') + '/Pca/pca_envfit_vector.xls'
                        insert_table_detail(env_vec_path, 'vector', update_id=main_id)
                        insert_text_detail(env_vec_pr_path, 'env_vector', main_id=main_id)
                else:
                    pass
                self.bind_object.logger.info('beta_diversity:PCA分析结果导入数据库完成.')
            elif analysis == 'pcoa':
                site_path = dir_path.rstrip('/') + '/Pcoa/pcoa_sites.xls'
                insert_table_detail(site_path, 'specimen', update_id=main_id)
                # rotation_path = dir_path.rstrip('/') + '/Pcoa/pcoa_rotation.xls'
                importance_path = dir_path.rstrip('/') + '/Pcoa/pcoa_eigenvalues.xls'
                # insert_text_detail(rotation_path, 'rotation', main_id=main_id)
                insert_text_detail(importance_path, 'eigenvalues', main_id=main_id)
                self.bind_object.logger.info('beta_diversity:PCoA分析结果导入数据库完成.')
            elif analysis == 'nmds':
                site_path = dir_path.rstrip('/') + '/Nmds/nmds_sites.xls'
                insert_table_detail(site_path, 'specimen', update_id=main_id)
                self.bind_object.logger.info('beta_diversity:NMDS分析结果导入数据库完成.')
            elif analysis == 'dbrda':
                site_path = dir_path.rstrip('/') + '/Dbrda/db_rda_sites.xls'
                # species_path = dir_path.rstrip('/') + '/Dbrda/db_rda_species.xls'
                insert_table_detail(site_path, 'specimen', update_id=main_id)
                # if os.path.exists(species_path):
                #     insert_table_detail(species_path, 'species', update_id=main_id)
                filelist = os.listdir(dir_path.rstrip('/') + '/Dbrda')
                if 'db_rda_centroids.xls' in filelist:
                    env_fac_path = dir_path.rstrip('/') + '/Dbrda/db_rda_centroids.xls'
                    insert_table_detail(env_fac_path, 'factor', update_id=main_id)
                if 'db_rda_biplot.xls' in filelist:
                    env_vec_path = dir_path.rstrip('/') + '/Dbrda/db_rda_biplot.xls'
                    insert_table_detail(env_vec_path, 'vector', update_id=main_id)
                self.bind_object.logger.info('beta_diversity:db_RDA分析结果导入数据库完成.')
            elif analysis == 'rda_cca':
                if 'rda' in os.listdir(dir_path.rstrip('/') + '/Rda/')[1]:
                    rda_cca = 'rda'
                else:
                    rda_cca = 'cca'
                site_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_sites.xls'
                species_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_species.xls'
                importance_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_importance.xls'
                dca_path = dir_path.rstrip('/') + '/Rda/' + 'dca.xls'
                insert_table_detail(site_path, 'specimen', update_id=main_id)
                insert_table_detail(species_path, 'species', update_id=main_id)
                insert_text_detail(importance_path, 'importance', main_id=main_id)
                insert_text_detail(dca_path, 'dca', main_id=main_id)
                filelist = os.listdir(dir_path.rstrip('/') + '/Rda')
                if (rda_cca + '_centroids.xls') in filelist:
                    env_fac_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_centroids.xls'
                    insert_table_detail(env_fac_path, 'factor', update_id=main_id)
                if (rda_cca + '_biplot.xls') in filelist:
                    env_vec_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_biplot.xls'
                    insert_table_detail(env_vec_path, 'vector', update_id=main_id, fileter_biplot=remove)
                self.bind_object.logger.info('beta_diversity:RDA/CCA分析结果导入数据库完成.')
            elif analysis == 'plsda':
                site_path = dir_path.rstrip('/') + '/Plsda/plsda_sites.xls'
                rotation_path = dir_path.rstrip('/') + '/Plsda/plsda_rotation.xls'
                importance_path = dir_path.rstrip('/') + '/Plsda/plsda_importance.xls'
                insert_table_detail(site_path, 'specimen', update_id=main_id, remove_key_blank=True)
                insert_text_detail(rotation_path, 'rotation', main_id=main_id)
                insert_text_detail(importance_path, 'importance', main_id=main_id)
            else:
                raise Exception('提供的analysis：%s不存在' % analysis)
                self.bind_object.logger.info('beta_diversity:PLSDA分析结果导入数据库完成.')
        else:
            raise Exception('提供的_id：%s在sg_beta_multi_analysis中无法找到表' % str(main_id))
        return main_id
