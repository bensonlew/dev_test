# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import json
from biocluster.api.database.base import Base, report_check
import re
import datetime
from bson import SON
from biocluster.config import Config


class Pca(Base):
    """
    """
    def __init__(self, bind_object):
        super(Pca, self).__init__(bind_object)
        self.output_dir = self.bind_object.output_dir
        if Config().MONGODB == 'sanger':
            self._db_name = 'toolapps'
        else:
            self._db_name = 'ttoolapps'
        self.check()

    @report_check
    def run(self):
        """
        运行函数
        """
        self.main_id = self.scatter_in()
        self.table_ids = self.table_in()
        # self.main_in()
        # self.detail_in()
        return self.main_id
        pass

    def table_in(self):
        """
        导入表格相关信息
        """
        ratation = self.insert_table(self.output_dir + '/pca_rotation.xls', 'PCA_特征主成分贡献度表',
                                     'PCA分析中样本特征的贡献度统计结果，例如在OTU表的PCA分析中代表物种/OTU的贡献度')
        importance = self.insert_table(self.output_dir + '/pca_importance.xls', 'PCA_解释度表', 'PCA结果坐标轴的解释度值')
        return [ratation, importance]

    def insert_table(self, fp, name, desc):
        with open(fp) as f:
            columns = f.readline().strip().split('\t')
            insert_data = []
            table_id = self.db['table'].insert_one(SON(
                project_sn=self.bind_object.sheet.project_sn,
                task_id=self.bind_object.id,
                name=name,
                attrs=columns,
                desc=desc,
                status='end',
                created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )).inserted_id
            for line in f:
                line_split = line.strip().split('\t')
                data = dict(zip(columns, line_split))
                data['table_id'] = table_id
                insert_data.append(data)
            self.db['table_detail'].insert_many(insert_data)
        return table_id

    def scatter_in(self):
        """
        导入散点图相关信息
        """
        with open(self.output_dir + '/pca_sites.xls') as f:
            attrs = f.readline().strip().split('\t')[1:]
            samples = []
            insert_data = []
            scatter_id = self.db['scatter'].insert_one(SON(
                project_sn=self.bind_object.sheet.project_sn,
                task_id=self.bind_object.id,
                name='pca',
                attrs=attrs,
                desc='PCA主成分分析',
                status='faild',
                created_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )).inserted_id
            for line in f:
                line_split = line.strip().split('\t')
                samples.append(line_split[0])
                data = SON(specimen_name=line_split[0],
                           scatter_id=scatter_id)
                data.update(zip(attrs, line_split[1:]))
                insert_data.append(data)
            specimen_ids_dict = self.insert_specimens(samples)
            for i in insert_data:
                i['specimen_id'] = specimen_ids_dict[i['specimen_name']]
            self.db['scatter_detail'].insert_many(insert_data)
            reverse_ids = SON([(str(n), m) for m, n in specimen_ids_dict.iteritems()])
            self.db['specimen_group'].insert_one(SON(data=[('project_sn', self.bind_object.sheet.project_sn),
                                                           ('task_id', self.bind_object.id),
                                                           ('visual_type', ['scatter']),
                                                           ('visual_id', [scatter_id]),
                                                           ('name', 'ALL'),
                                                           ('category_names', ['ALL']),
                                                           ('specimen', [reverse_ids])]))
            self.db['scatter'].update_one({'_id': scatter_id}, {'$set': {'status': 'end', 'specimen_ids': specimen_ids_dict.values()}})
            return scatter_id

    def insert_specimens(self, specimen_names):
        """
        """
        task_id = self.bind_object.id
        project_sn = self.bind_object.sheet.project_sn
        datas = [SON(project_sn=project_sn, task_id=task_id, name=i) for i in specimen_names]
        ids = self.db['specimen'].insert_many(datas).inserted_ids
        return SON(zip(specimen_names, ids))



    def check(self):
        """
        检查文件格式是否正确
        """
        pass

    def main_in(self):
        """
        分析基础任务信息导入数据库函数
        """
        main_data = {
            'project_sn': self.bind_object.sheet.project_sn,
            'status': 'end',
            'task_id': self.bind_object.id,
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'name': 'pca',
            'desc': 'PCA主成分分析',
            'params': json.dumps(self.bind_object.sheet.options(), sort_keys=True, separators=(',', ':'))
        }
        main_id = self.db['pca'].insert_one(main_data).inserted_id
        self.main_id = main_id
        pass

    def detail_in(self):
        """
        分析具体文件内容信息导入数据库函数
        """
        site_path = self.output_dir + '/pca_sites.xls'
        rotation_path = self.output_dir + '/pca_rotation.xls'
        importance_path = self.output_dir + '/pca_importance.xls'
        self.table_insert(site_path, self.main_id, table_type='specimen')
        self.table_insert(rotation_path, self.main_id, table_type='species', split_fullname=True)
        self.table_insert(importance_path, self.main_id, table_type='importance', colls=['proportion_variance'])
        pass

    def table_insert(self, file_path, update_id, table_type=None,
                     coll_name='pca_detail',
                     main_coll='pca',
                     update_column=True,
                     fileter_biplot=None,
                     remove_key_blank=False,
                     split_fullname=False,
                     colls=None,
                     header_first='name',
                     raise_NA=False):
        """
        表格导入函数

        :params file_path:文件路径
        :params update_id:主表ID
        :params table_type:表格类型名称，作为表格标签写入doc: {type: 'species'}, 配合update_column更新主表，添加此字段，值为本次插入键名(逗号分隔)
        :params coll_name:表collection名称
        :params main_coll:主表collection名称
        :params update_column:更新主表添加table_type值字段:{'species': 'Aaa,Bbb,Ccc'}
        :params fileter_biplot:过滤特殊字段开头的的行，list
        :params remove_key_blank:去除键(表头)中的空格
        :params split_fullname:生成简单行名称
        :params colls:新键(表头) 注意：不带第一个行头
        :params header_first: 行头名称，默认为name
        :params raise_NA:当数据没有实际值是否报错
        """
        collection = self.db[coll_name]
        with open(file_path, 'rb') as f:
            all_lines = f.readlines()
            columns = all_lines[0].rstrip().split('\t')[1:]
            if remove_key_blank:
                columns = [i.replace(' ', '') for i in columns]
            if colls:
                if len(colls) != len(columns):
                    raise Exception('提供的表头字段与数据不匹配')
                columns = colls
            data_temp = []
            if fileter_biplot:
                if not isinstance(fileter_biplot, list):
                    raise Exception('需要删除的匹配列必须为列表')
            for line in all_lines[1:]:
                values = line.rstrip().split('\t')
                if fileter_biplot:
                    flag = 0
                    for i in fileter_biplot:
                        if re.match(r'^{}'.format(i), values[0]):
                            flag = 1
                            break
                    if flag:
                        continue
                if main_coll:
                    main_id = main_coll + '_id'
                else:
                    main_id = 'main_id'
                insert_data = {
                    main_id: update_id
                }
                if table_type:
                    insert_data['type'] = table_type
                if split_fullname:
                    insert_data['full' + header_first] = values[0]
                    insert_data[header_first] = values[0].split(';')[-1].strip()
                else:
                    insert_data[header_first] = values[0]
                values_dict = dict(zip(columns, values[1:]))
                data_temp.append(dict(insert_data, **values_dict))
            if data_temp:
                collection.insert_many(data_temp)
            else:
                if raise_NA:
                    raise Exception('数据内容为空！')
                else:
                    return None
            if update_column:
                main_collection = self.db[main_coll]
                main_collection.update_one({'_id': update_id},
                                           {'$set': {table_type: ','.join(columns)}},
                                           upsert=False)
