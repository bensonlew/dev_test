# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import os
from pymongo import MongoClient
from biocluster.config import Config
from bson.objectid import ObjectId


client = MongoClient(Config().MONGO_URI)
db = client["sanger"]


def export_otu_table(data, option_name, dir_path, bind_obj=None):
    file_path = os.path.join(dir_path, "%s_input.otu.xls" % option_name)
    bind_obj.logger.debug("正在导出参数%s的OTU表格为文件，路径:%s" % (option_name, file_path))
    collection = db['sg_otu']
    result = collection.find_one({"_id": ObjectId(data)})
    samples = result["specimen_names"]
    collection = db['sg_otu_detail']
    with open(file_path,"wb") as f:
        f.write("OTU ID\t%s\ttaxonomy\n" % "\t".join(samples))
        for col in collection.find({"_id": ObjectId(data)}):
            line = "%s" % col["otu"]
            for s in samples:
                line += "\t%s" % col[s]
            for cls in ["d__", "k__", "p__", "c__", "o__", "f__", "g__"]:
                if cls in col.keys():
                    line += "\t%s; " % col[cls]
            f.write("%s\n" % line)
    return file_path
