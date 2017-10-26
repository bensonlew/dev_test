# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import os
import json
import datetime
import re
from biocluster.api.database.base import Base, report_check
from bson.objectid import ObjectId
from types import StringTypes
# from biocluster.config import Config
from mainapp.libs.param_pack import group_detail_sort
# import re
# import datetime
# from bson.son import SON


class BetaMultiAnalysis(Base):
    def __init__(self, bind_object):
        super(BetaMultiAnalysis, self).__init__(bind_object)
        self._project_type = 'meta'
        # self._db_name = Config().MONGODB
        self._tables = []  # 记录存入了哪些表格

    @staticmethod
    def get_main_table_name(analysis_type):
        if analysis_type == 'pca':
            return 'PCA'
        elif analysis_type == 'pcoa':
            return 'PCoA'
        elif analysis_type == 'nmds':
            return 'NMDS'
        elif analysis_type == 'plsda':
            return 'PLS-DA'
        elif analysis_type == 'dbrda':
            return 'db-RDA'
        elif analysis_type == 'rda_cca':
            return 'RDA/CCA'
        else:
            raise Exception('错误的分析类型')

    @report_check
    def add_beta_multi_analysis_result(self, dir_path, analysis, main_id=None, main=False, env_id=None, group_id=None,
                                       task_id=None, otu_id=None, name=None, params=None, level=9, remove=None,
                                       spname_spid=None):
        self._tables = []  # 记录存入了哪些表格
        if level and level not in range(1, 10):
            raise Exception("level参数%s为不在允许范围内!" % level)
        if task_id is None:
            task_id = self.bind_object.sheet.id
        if not isinstance(env_id, ObjectId) and env_id is not None:
            env_id = ObjectId(env_id)
        else:
            if 'env_id' in self.bind_object.sheet.data['options']:
                env_id = ObjectId(self.bind_object.option('env_id'))  # 仅仅即时计算直接绑定workflow对象
        if not isinstance(group_id, ObjectId) and group_id is not None:
            group_id = ObjectId(group_id)
        else:
            if 'group_id' in self.bind_object.sheet.data['options']:
                group_id = self.bind_object.option('group_id')
                if group_id not in ['all', 'All', 'ALL']:
                    group_id = ObjectId(group_id)  # 仅仅即时计算直接绑定workflow对象
            else:
                group_id = 'all'
        if isinstance(otu_id, ObjectId):
            pass
        elif otu_id is not None:
            otu_id = ObjectId(otu_id)
        else:
            otu_id = ObjectId(self.bind_object.option('otu_id'))  # 仅仅即时计算直接绑定workflow对象
        _main_collection = self.db['sg_beta_multi_analysis']
        if main:
            if not isinstance(params, dict):
                params_dict = json.loads(params)
            else:
                params_dict = params
            if env_id:
                if isinstance(params, dict):
                    params_dict['env_id'] = str(env_id)    # env_id在再metabase中不可用
            params_dict['otu_id'] = str(otu_id)  # otu_id在再metabase中不可用
            if spname_spid:
                params_dict['group_id'] = group_id
                group_detail = {'All': [str(i) for i in spname_spid.values()]}
                params_dict['group_detail'] = group_detail_sort(group_detail)
            insert_mongo_json = {
                'project_sn': self.bind_object.sheet.project_sn,
                'task_id': task_id,
                'otu_id': otu_id,
                'level_id': int(level),
                'name': BetaMultiAnalysis.get_main_table_name(analysis) + '_Origin',
                'table_type': analysis,
                'env_id': env_id,
                'group_id': group_id,
                'params': (json.dumps(params_dict, sort_keys=True, separators=(',', ':'))
                           if isinstance(params, dict) else params),
                'status': 'end',
                'desc': '',
                'created_ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            if analysis == 'rda_cca':  # 在主表中添加必要的rda或者是cca分类信息
                rda_files = os.listdir(dir_path.rstrip('/') + '/Rda')
                if 'cca_sites.xls' in rda_files:
                    insert_mongo_json['rda_cca'] = 'cca'
                elif 'rda_sites.xls' in rda_files:
                    insert_mongo_json['rda_cca'] = 'rda'
                else:
                    raise Exception('RDA/CCA分析没有生成正确的结果数据')
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
                self.insert_table_detail(site_path, 'specimen', update_id=main_id)
                self.insert_table_detail(rotation_path, 'species', update_id=main_id, split_fullname=True)
                self.insert_table_detail(importance_path, 'importance', update_id=main_id, colls=['proportion_variance'])
                if result['env_id']:
                    filelist = os.listdir(dir_path.rstrip('/') + '/Pca')
                    if 'pca_envfit_factor_scores.xls' in filelist:
                        env_fac_path = dir_path.rstrip('/') + '/Pca/pca_envfit_factor_scores.xls'
                        env_fac_pr_path = dir_path.rstrip('/') + '/Pca/pca_envfit_factor.xls'
                        self.insert_table_detail(env_fac_path, 'factor', update_id=main_id)
                        self.insert_table_detail(env_fac_pr_path, 'factor_stat', update_id=main_id)
                    if 'pca_envfit_vector_scores.xls' in filelist:
                        env_vec_path = dir_path.rstrip('/') + '/Pca/pca_envfit_vector_scores.xls'
                        env_vec_pr_path = dir_path.rstrip('/') + '/Pca/pca_envfit_vector.xls'
                        self.insert_table_detail(env_vec_path, 'vector', update_id=main_id)
                        self.insert_table_detail(env_vec_pr_path, 'vector_stat', update_id=main_id)
                else:
                    pass
                self.bind_object.logger.info('beta_diversity:PCA分析结果导入数据库完成.')
            elif analysis == 'pcoa':
                site_path = dir_path.rstrip('/') + '/Pcoa/pcoa_sites.xls'
                self.insert_table_detail(site_path, 'specimen', update_id=main_id)
                importance_path = dir_path.rstrip('/') + '/Pcoa/pcoa_eigenvalues.xls'
                self.insert_table_detail(importance_path, 'importance', update_id=main_id)
                importance_pre_path = dir_path.rstrip('/') + '/Pcoa/pcoa_eigenvaluespre.xls'
                self.insert_table_detail(importance_pre_path, 'importancepre', update_id=main_id)
                self.bind_object.logger.info('beta_diversity:PCoA分析结果导入数据库完成.')
            elif analysis == 'nmds':
                site_path = dir_path.rstrip('/') + '/Nmds/nmds_sites.xls'
                self.insert_table_detail(site_path, 'specimen', update_id=main_id, colls=['NMDS1', 'NMDS2'])  # 这里暂且只有两个轴，故只写两个，后面如果修改进行多维NMDS，结果应该有多列，此种方式应该进行修改
                nmds_stress = float(open(dir_path.rstrip('/') + '/Nmds/nmds_stress.xls').readlines()[1])
                _main_collection.update_one({'_id': main_id}, {'$set': {'nmds_stress': nmds_stress}})
                self.bind_object.logger.info('beta_diversity:NMDS分析结果导入数据库完成.')
            elif analysis == 'dbrda':
                site_path = dir_path.rstrip('/') + '/Dbrda/db_rda_sites.xls'
                self.insert_table_detail(site_path, 'specimen', update_id=main_id)
                filelist = os.listdir(dir_path.rstrip('/') + '/Dbrda')
                if 'db_rda_centroids.xls' in filelist:
                    env_fac_path = dir_path.rstrip('/') + '/Dbrda/db_rda_centroids.xls'
                    self.insert_table_detail(env_fac_path, 'factor', update_id=main_id)
                if 'db_rda_biplot.xls' in filelist:
                    env_vec_path = dir_path.rstrip('/') + '/Dbrda/db_rda_biplot.xls'
                    self.insert_table_detail(env_vec_path, 'vector', update_id=main_id)
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
                plot_species_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_plot_species_data.xls'  # add 4 lines by zhouxuan 20170123 20170401
                envfit_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_envfit.xls'
                if os.path.exists(envfit_path):
                    self.insert_envfit_table(envfit_path, 'envfit', update_id=main_id)
                self.insert_table_detail(plot_species_path, 'plot_species', update_id=main_id)
                self.insert_table_detail(site_path, 'specimen', update_id=main_id)
                self.insert_table_detail(species_path, 'species', update_id=main_id, split_fullname=True)
                self.insert_table_detail(importance_path, 'importance', update_id=main_id, colls=['proportion_variance'])
                self.insert_table_detail(dca_path, 'dca', update_id=main_id)
                filelist = os.listdir(dir_path.rstrip('/') + '/Rda')
                if (rda_cca + '_centroids.xls') in filelist:
                    env_fac_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_centroids.xls'
                    self.insert_table_detail(env_fac_path, 'factor', update_id=main_id)
                if (rda_cca + '_biplot.xls') in filelist:
                    env_vec_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_biplot.xls'
                    self.insert_table_detail(env_vec_path, 'vector', update_id=main_id, fileter_biplot=remove)
                self.bind_object.logger.info('beta_diversity:RDA/CCA分析结果导入数据库完成.')
            elif analysis == 'plsda':
                site_path = dir_path.rstrip('/') + '/Plsda/plsda_sites.xls'
                rotation_path = dir_path.rstrip('/') + '/Plsda/plsda_rotation.xls'
                importance_path = dir_path.rstrip('/') + '/Plsda/plsda_importance.xls'
                importance_pre_path = dir_path.rstrip('/') + '/Plsda/plsda_importancepre.xls'
                self.insert_table_detail(site_path, 'specimen', update_id=main_id, remove_key_blank=True)
                self.insert_table_detail(rotation_path, 'species', update_id=main_id, remove_key_blank=True, split_fullname=True)
                self.insert_table_detail(importance_path, 'importance', update_id=main_id, remove_key_blank=True)
                self.insert_table_detail(importance_pre_path, 'importancepre', update_id=main_id, colls=['proportion_variance'])
                self.bind_object.logger.info('beta_diversity:PLSDA分析结果导入数据库完成.')
            else:
                raise Exception('提供的analysis：%s不存在' % analysis)
                self.bind_object.logger.info('beta_diversity:PLSDA分析结果导入数据库完成.')
            self.insert_main_tables(self._tables, update_id=main_id)
        else:
            raise Exception('提供的_id：%s在sg_beta_multi_analysis中无法找到表' % str(main_id))
        return main_id

    def insert_table_detail(self, file_path, table_type, update_id,
                            coll_name='sg_beta_multi_analysis_detail',
                            main_coll='sg_beta_multi_analysis',
                            update_column=True, db=None, fileter_biplot=None, remove_key_blank=False,
                            split_fullname=False, colls=None):
        self._tables.append(table_type)
        if not db:
            db = self.db
        collection = db[coll_name]
        with open(file_path, 'rb') as f:
            all_lines = f.readlines()
            columns = all_lines[0].rstrip().split('\t')[1:]
            if remove_key_blank:
                columns = [i.replace(' ', '') for i in columns]
            if colls:
                columns = colls
            data_temp = []
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
                    'type': table_type
                }
                if split_fullname:
                    insert_data['fullname'] = values[0]
                    insert_data['name'] = values[0].split(';')[-1].strip()
                else:
                    insert_data['name'] = values[0]
                values_dict = dict(zip(columns, values[1:]))
                data_temp.append(dict(insert_data, **values_dict))
            if data_temp:
                collection.insert_many(data_temp)
            else:
                return None
            if update_column:
                main_collection = db[main_coll]
                # default_column = {'specimen': 'detail_column', 'factor': 'factor_column',
                #                   'vector': 'vector_column',
                #                   'species': 'species_column', 'rotation': 'rotation_column'}
                default_column = {'specimen': 'specimen', 'factor': 'factor', 'vector': 'vector',
                                  'species': 'species', 'factor_stat': 'factor_stat',
                                  'vector_stat': 'vector_stat',
                                  'importance': 'importance', 'dca': 'dca', 'importancepre': 'importancepre',
                                  'plot_species': 'plot_species'}
                if table_type in default_column:
                    main_collection.update_one({'_id': update_id},
                                               {'$set': {default_column[table_type]: ','.join(columns)}},
                                               upsert=False)
                else:
                    raise Exception('错误的表格类型：%s不能在主表中插入相应表头' % table_type)

    def insert_text_detail(self, file_path, data_type, main_id,
                           coll_name='sg_beta_multi_analysis_json_detail', db=None):
        if not db:
            db = self.db
        collection = db[coll_name]
        with open(file_path, 'rb') as f:
            data = f.read()
            insert_data = {
                'multi_analysis_id': main_id,
                'type': data_type,
                'json_value': data
            }
            collection.insert_one(insert_data)

    def insert_main_tables(self, tables, update_id, main='sg_beta_multi_analysis'):
        """
        """
        main_collection = self.db[main]
        main_collection.update_one({'_id': update_id},
                                   {'$set': {'tables': ','.join(tables)}},
                                   upsert=False)

    def insert_envfit_table(self, filepath, tabletype, update_id):
            """
			"""
            insert_data = []
            with open(filepath,'rb') as r:
                head = r.next().strip('\r\n')
                head = re.split(' ', head)
                new_head_old = head
                new_head =[]
                for f in range(0, len(new_head_old)):
                    pattern = re.compile('"(.*)"')
                    new_head_ = pattern.findall(new_head_old[f])
                    new_head.append(new_head_[0])
                    # m = re.match('/\"(.*)\"/', new_head[f])
                    # if m:
                    #     new_head[f] = m.group(0)
                print(new_head)
                new_head[-1] = "p_values"
                new_head[-2] = "r2"
                self.new_head = new_head
                for line in r:
                    line = line.rstrip("\r\n")
                    line = re.split(' ', line)
                    content = line[1:]
                    pattern = re.compile('"(.*)"')
                    env_name = pattern.findall(line[0])
                    env_detail = dict()
                    for i in range(0, len(content)):
                        env_detail[new_head[i]] = content[i]
                    env_detail['multi_analysis_id'] = update_id
                    env_detail['type'] = tabletype
                    env_detail['env'] = env_name[0]
                    insert_data.append(env_detail)
            try:
                collection = self.db['sg_beta_multi_analysis_detail']
                collection.insert_many(insert_data)
            except Exception as e:
                self.bind_object.logger.error("导入sg_beta_multi_analysis_detail表格信息出错:{}".format(e))
            else:
                self.bind_object.logger.info("导入sg_beta_multi_analysis_detail表格成功")
            self._tables.append(tabletype)
            # self.insert_main_tables(self._tables, update_id)
            main_collection = self.db['sg_beta_multi_analysis']
            main_collection.update_one({'_id': update_id},
                                       {'$set': {tabletype: ','.join(self.new_head)}},
                                       upsert=False)








    # @report_check
    # def add_beta_multi_analysis_result_for_api(self, dir_path, analysis, main_id):
    #     if analysis == 'pca':
    #         site_path = dir_path.rstrip('/') + '/Pca/pca_sites.xls'
    #         rotation_path = dir_path.rstrip('/') + '/Pca/pca_rotation.xls'
    #         importance_path = dir_path.rstrip('/') + '/Pca/pca_importance.xls'
    #         self.insert_table_detail(site_path, 'specimen', update_id=main_id)
    #         self.insert_table_detail(rotation_path, 'species', update_id=main_id, split_fullname=True)
    #         self.insert_table_detail(importance_path, 'importance', update_id=main_id, colls=['proportion_variance'])
    #         if result['env_id']:
    #             filelist = os.listdir(dir_path.rstrip('/') + '/Pca')
    #             if 'pca_envfit_factor_scores.xls' in filelist:
    #                 env_fac_path = dir_path.rstrip('/') + '/Pca/pca_envfit_factor_scores.xls'
    #                 env_fac_pr_path = dir_path.rstrip('/') + '/Pca/pca_envfit_factor.xls'
    #                 self.insert_table_detail(env_fac_path, 'factor', update_id=main_id)
    #                 self.insert_table_detail(env_fac_pr_path, 'factor_stat', update_id=main_id)
    #             if 'pca_envfit_vector_scores.xls' in filelist:
    #                 env_vec_path = dir_path.rstrip('/') + '/Pca/pca_envfit_vector_scores.xls'
    #                 env_vec_pr_path = dir_path.rstrip('/') + '/Pca/pca_envfit_vector.xls'
    #                 self.insert_table_detail(env_vec_path, 'vector', update_id=main_id)
    #                 self.insert_table_detail(env_vec_pr_path, 'vector_stat', update_id=main_id)
    #         else:
    #             pass
    #         self.bind_object.logger.info('beta_diversity:PCA分析结果导入数据库完成.')
    #     elif analysis == 'pcoa':
    #         site_path = dir_path.rstrip('/') + '/Pcoa/pcoa_sites.xls'
    #         self.insert_table_detail(site_path, 'specimen', update_id=main_id)
    #         importance_path = dir_path.rstrip('/') + '/Pcoa/pcoa_eigenvalues.xls'
    #         self.insert_table_detail(importance_path, 'importance', update_id=main_id)
    #         self.bind_object.logger.info('beta_diversity:PCoA分析结果导入数据库完成.')
    #     elif analysis == 'nmds':
    #         site_path = dir_path.rstrip('/') + '/Nmds/nmds_sites.xls'
    #         self.insert_table_detail(site_path, 'specimen', update_id=main_id, colls=['NMDS1', 'NMDS2'])  # 这里暂且只有两个轴，故只写两个，后面如果修改进行多维NMDS，结果应该有多列，此种方式应该进行修改
    #         self.bind_object.logger.info('beta_diversity:NMDS分析结果导入数据库完成.')
    #     elif analysis == 'dbrda':
    #         site_path = dir_path.rstrip('/') + '/Dbrda/db_rda_sites.xls'
    #         self.insert_table_detail(site_path, 'specimen', update_id=main_id)
    #         filelist = os.listdir(dir_path.rstrip('/') + '/Dbrda')
    #         if 'db_rda_centroids.xls' in filelist:
    #             env_fac_path = dir_path.rstrip('/') + '/Dbrda/db_rda_centroids.xls'
    #             self.insert_table_detail(env_fac_path, 'factor', update_id=main_id)
    #         if 'db_rda_biplot.xls' in filelist:
    #             env_vec_path = dir_path.rstrip('/') + '/Dbrda/db_rda_biplot.xls'
    #             self.insert_table_detail(env_vec_path, 'vector', update_id=main_id)
    #         self.bind_object.logger.info('beta_diversity:db_RDA分析结果导入数据库完成.')
    #     elif analysis == 'rda_cca':
    #         if 'rda' in os.listdir(dir_path.rstrip('/') + '/Rda/')[1]:
    #             rda_cca = 'rda'
    #         else:
    #             rda_cca = 'cca'
    #         site_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_sites.xls'
    #         species_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_species.xls'
    #         importance_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_importance.xls'
    #         dca_path = dir_path.rstrip('/') + '/Rda/' + 'dca.xls'
    #         self.insert_table_detail(site_path, 'specimen', update_id=main_id)
    #         self.insert_table_detail(species_path, 'species', update_id=main_id, split_fullname=True)
    #         self.insert_table_detail(importance_path, 'importance', update_id=main_id, colls=['proportion_variance'])
    #         self.insert_table_detail(dca_path, 'dca', update_id=main_id)
    #         filelist = os.listdir(dir_path.rstrip('/') + '/Rda')
    #         if (rda_cca + '_centroids.xls') in filelist:
    #             env_fac_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_centroids.xls'
    #             self.insert_table_detail(env_fac_path, 'factor', update_id=main_id)
    #         if (rda_cca + '_biplot.xls') in filelist:
    #             env_vec_path = dir_path.rstrip('/') + '/Rda/' + rda_cca + '_biplot.xls'
    #             self.insert_table_detail(env_vec_path, 'vector', update_id=main_id, fileter_biplot=remove)
    #         self.bind_object.logger.info('beta_diversity:RDA/CCA分析结果导入数据库完成.')
    #     elif analysis == 'plsda':
    #         site_path = dir_path.rstrip('/') + '/Plsda/plsda_sites.xls'
    #         rotation_path = dir_path.rstrip('/') + '/Plsda/plsda_rotation.xls'
    #         importance_path = dir_path.rstrip('/') + '/Plsda/plsda_importance.xls'
    #         self.insert_table_detail(site_path, 'specimen', update_id=main_id, remove_key_blank=True)
    #         self.insert_table_detail(rotation_path, 'species', update_id=main_id, remove_key_blank=True, split_fullname=True)
    #         self.insert_table_detail(importance_path, 'importance', update_id=main_id, remove_key_blank=True)
    #         self.bind_object.logger.info('beta_diversity:PLSDA分析结果导入数据库完成.')
    #     else:
    #         raise Exception('提供的analysis：%s不存在' % analysis)
    #         self.bind_object.logger.info('beta_diversity:PLSDA分析结果导入数据库完成.')
    #     self.insert_main_tables(self._tables, update_id=main_id)
    #     return main_id
