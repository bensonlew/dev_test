# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
import os
import re
from collections import defaultdict
from pymongo import MongoClient
from biocluster.config import Config
from bson.objectid import ObjectId
import copy


client = MongoClient(Config().MONGO_URI)
db = client["sanger"]


def export_distance_matrix(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "%s_input.matrix.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的距离矩阵为文件，路径:%s" % (option_name, file_path))
    collection = db['sg_beta_specimen_distance_detail']
    results = collection.find({"specimen_distance_id": ObjectId(data)})
    samples = []
    copysamples = copy.deepcopy(samples)
    for result in results:
        samples.append(result["specimen_name"])
    with open(file_path, "wb") as f:
        f.write("\t%s\n" % "\t".join(samples))
        for sample in samples:
            doc = {}
            value = []
            for result in results:
                if result['specimen_name'] == sample:
                    doc = result
                    break
            for detail in copysamples:
                value.append(doc[detail])
            f.write(sample + '\t' + '\t'.join(value) + '\n')
    return file_path
