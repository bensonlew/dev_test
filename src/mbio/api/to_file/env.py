# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import os
# import copy
# import json
from biocluster.config import Config
from bson.objectid import ObjectId
# from collections import defaultdict


client = Config().mongo_client
db = client['sanger']


def export_env_table(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, '%s_input_env.xls' % option_name)
    bind_obj.logger.debug('正在导出参数%s的环境因子表为文件，路径:%s' % (option_name, file_path))
    collection_main = db['sg_env']
    result_main = collection_main.find_one({'_id': ObjectId(data)})
    if not result_main:
        raise Exception('环境因子id没有找到对应的表信息')
    all_envs = result_main['env_names'].strip().split(',')
    collection = db['sg_env_detail']
    results = collection.find({'env_id': ObjectId(data)})
    if not results:
        raise Exception('环境因子id没有找到对应的detail数据')
    bind_obj.logger.info('ALL ENVS:' + ' '.join(all_envs))
    with open(file_path, 'wb') as f:
        f.write('\t' + '\t'.join(all_envs) + '\n')
        for one in results:
            line_list = [one['specimen_name']]
            for env in all_envs:
                line_list.append(str(one[env]))
            f.write('\t'.join(line_list) + '\n')
    return file_path
