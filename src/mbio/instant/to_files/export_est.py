# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import os
from pymongo import MongoClient
from biocluster.config import Config
from bson.objectid import ObjectId
import json


client = MongoClient(Config().MONGO_URI)
db = client["sanger"]


def export_est_table(est_id, target_path):
    est_path = os.path.join(target_path, "estimators.xls")
    file_path = os.path.join(target_path, "estimators_for_t.xls")
    cmd_path = os.path.join(target_path, "cmd.r")
    print("正在导出多样性指数表格为文件，路径:%s" % file_path)
    collection = db['sg_alpha_diversity_detail']
    est_collection = db['sg_alpha_diversity']
    result = est_collection.find_one({"_id": ObjectId(est_id)})
    if not result:
        raise Exception('没有找到多样性指数id对应的表，请检查传入的id是否正确')
    print(type(result['params']))
    if not result['params']:
        index_type = u"ace,chao,shannon,simpson,coverage"
    elif type(result['params']) is dict:
        params = result["params"]
        index_type = params['indices']
    else:
        params = json.loads(result["params"])
        index_type = params['indices']
    indices = index_type.split(',')
    details = collection.find({"alpha_diversity_id": ObjectId(est_id)})
    if not details.count():
        raise Exception('没有找到相应detail信息')
    with open(est_path, "wb") as f:
        # f.write('index_type')
        for index in indices:
            f.write('\t%s' % index)
        f.write('\n')
        for col in details:
            line = '%s' % col['specimen_name']
            for index in indices:
                line += '\t%s' % col[index]
                # bind_obj.logger.debug(line)
            f.write('%s\n' % line)
    test = '''
    table <- read.table("'''+est_path+'''",sep = '\t')
    table <- t(table)
    write.table(table, "'''+file_path+'''",sep = '\t', row.names = F, col.names = F)'''
    with open(cmd_path, 'wb') as r:
        r.write('%s' % test)
    os.system('/mnt/ilustre/users/sanger/app/R-3.2.2/bin/Rscript %s' % cmd_path)
    return file_path

