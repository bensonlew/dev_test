# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
import os
from pymongo import MongoClient
from biocluster.config import Config
from bson.objectid import ObjectId
import json


client = MongoClient(Config().MONGO_URI)
db = client[Config().MONGODB]


def export_est_table(data, option_name, dir_path, bind_obj=None):
    est_path = os.path.join(dir_path, "%s_input.estimators.xls" % option_name)
    file_path = os.path.join(dir_path, "%s_input.est_for_t.xls" % option_name)
    cmd_path = os.path.join(dir_path, "cmd.r")
    bind_obj.logger.debug("正在导出参数%s的多样性指数表格为文件，路径:%s" % (option_name, file_path))
    collection = db['sg_alpha_diversity_detail']
    est_collection = db['sg_alpha_diversity']
    result = est_collection.find_one({"_id": ObjectId(data)})
    if not result:
        raise Exception('没有找到多样性指数id对应的表，请检查传入的id是否正确')
    if not result['params']:
        index_type = u"ace,chao,shannon,simpson,coverage"
    elif type(result['params']) is dict:
        params = result["params"]
        if 'indices' in params:
            index_type = params['indices']
        elif 'index_type' in params:
            index_type = params['index_type']
    else:
        params = json.loads(result["params"])
        if 'indices' in params:
            index_type = params['indices']
        elif 'index_type' in params:
            index_type = params['index_type']
    indices = index_type.split(',')
    bind_obj.logger.debug(indices)
    details = collection.find({"alpha_diversity_id": ObjectId(data)})
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
                if index == "jack":
                    index = "jackknife"
                line += '\t%s' % col[index]
                # bind_obj.logger.debug(line)
            f.write('%s\n' % line)
    test = '''
    table <- read.table("'''+est_path+'''",sep = '\t')
    table <- t(table)
    write.table(table, "'''+file_path+'''",sep = '\t', row.names = F, col.names = F)'''
    with open(cmd_path, 'wb') as r:
        r.write('%s' % test)
    R_path = os.path.join(Config().SOFTWARE_DIR, "program/R-3.3.1/bin/Rscript")
    cmd = "{} {}".format(R_path, cmd_path)
    os.system(cmd)
    return file_path
