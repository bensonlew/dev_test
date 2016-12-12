# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import os
import re
import json
from types import StringTypes
from biocluster.config import Config
from bson.objectid import ObjectId
from collections import defaultdict


client = Config().mongo_client
db = client[Config().MONGODB]

def export_otu_table(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "otu_table.xls")
    bind_obj.logger.debug("正在导出参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))
    collection = db['sg_otu_specimen']
    my_collection = db['sg_specimen']
    results = collection.find({"otu_id": ObjectId(data)})
    samples = []
    for result in results:
        id_result = my_collection.find_one({"_id": result["specimen_id"]})
        if not id_result:
            raise Exception("意外错误，样本id:{}在sg_specimen中未找到！")
        samples.append(id_result["specimen_name"])
    # 因为有些样本名以1,2,3,4进行编号， 导致读出来了之后samples列表里的元素是数字， 需要先转化成字符串
    samples = map(str, samples)
    collection = db['sg_otu_detail']
    with open(file_path, "wb") as f:
        f.write("OTU ID\t%s\n" % "\t".join(samples))
        for col in collection.find({"otu_id": ObjectId(data)}):
            line = "%s\t" % col["otu"]
            for s in samples:
                line += "%s\t" % col[s]
            f.write("%s\n" % line)
    return file_path

def export_otu_rep(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "otu_reps.fasta")
    bind_obj.logger.debug("正在导出参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))
    my_collection = db['sg_otu']
    my_result = my_collection.find_one({'_id': ObjectId(data)})
    if not my_result:
        raise Exception("意外错误，otu_id:{}在sg_otu中未找到!".format(ObjectId(data)))
    collection = db['sg_otu_detail']
    with open(file_path, "wb") as f:
        for col in collection.find({"otu_id": ObjectId(data)}):
            line = ">%s\n" % col["otu"]
            line += col["otu_rep"]
            f.write("%s\n" % line)
    return file_path
