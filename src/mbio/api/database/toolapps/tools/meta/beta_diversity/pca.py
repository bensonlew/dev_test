# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import json
from biocluster.api.database.base import Base, report_check
import re
import datetime
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
        self.main_in()
        self.detail_in()
        return self.main_id
        pass

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
