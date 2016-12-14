# -*- coding: utf-8 -*-
# __author__ = 'zengjing'
import os
import re
import datetime
import json
from bson.son import SON
from bson.objectid import ObjectId
import types
import gridfs
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config


class FunctionPredict(Base):
    def __init__(self, bind_object):
        super(FunctionPredict, self).__init__(bind_object)
        self._db_name = Config().MONGODB

    @report_check
    def add_function_prediction(self, name=None, params=None, otu_id=0):
        if otu_id != 0 and not isinstance(otu_id, ObjectId):
            if isinstance(otu_id, types.StringTypes):
                otu_id = ObjectId(otu_id)
            else:
                raise Exception("otu_id必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_otu"]
        result = collection.find_one({"_id": otu_id})
        if not result:
            raise Exception("无法根据传入的_id:{}在sg_otu表里找到相应的记录".format(str(otu_id)))
        project_sn = result['project_sn']
        task_id = result['task_id']
        insert_data = {
            "project_sn": project_sn,
            "task_id": task_id,
            "name": name if name else "16s_function_prediction_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            "params": params,
            "status": "end",
            "desc": "16s功能预测主表",
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        collection = self.db["sg_16s_function_prediction"]
        prediction_id = collection.insert_one(insert_data).inserted_id
        return prediction_id

    @report_check
    def add_cog_function_prediction(self, prediction_id, sample_path, function_path, table_path):
        if not isinstance(prediction_id, ObjectId):
            if isinstance(prediction_id, types.StringTypes):
                prediction_id = ObjectId(prediction_id)
            else:
                raise Exception("prediction_id必须为ObjectId对象或其对应的字符串！")
        if not os.path.exists(sample_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(sample_path))
        if not os.path.exists(function_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(function_path))
        if not os.path.exists(table_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(table_path))
        data_list = []
        with open(sample_path, "rb") as s, open(table_path, "rb") as t:
            lines = s.readlines()
            line1 = t.readline().strip().split('\t')
            for line in lines[1:]:
                line = line.strip().split("\t")
                data = [
                    ("prediction_id", prediction_id),
                    ("type", "sample"),
                    ("cog_id", line[0]),
                    ("description", line[-1]),
                ]
                for i in range(1, len(line1)):
                    data += [
                        (line1[i], int(line[i]))
                    ]
                data = SON(data)
                data_list.append(data)
        with open(function_path, "rb") as f, open(table_path, "rb") as t:
            lines = f.readlines()
            line1 = t.readline().strip().split('\t')
            for line in lines[1:]:
                line = line.strip().split("\t")
                data = [
                    ("prediction_id", prediction_id),
                    ("type", "function"),
                    ("catergory", line[0]),
                    ("description", line[-1]),
                ]
                for i in range(1, len(line1)):
                    data += [
                        (line1[i], int(line[i]))
                    ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_cog_abundance_stat"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入cog 16s 功能预测%s、%s信息出错:%s" % (sample_path, function_path, e))
        else:
            self.bind_object.logger.info("导入cog 16s 功能预测%s、%s信息成功!" % (sample_path, function_path))

    @report_check
    def add_kegg_function_prediction(self, prediction_id, kegg_path, maps_path, table_path):
        if not isinstance(prediction_id, ObjectId):
            if isinstance(prediction_id, types.StringTypes):
                prediction_id = ObjectId(prediction_id)
            else:
                raise Exception("prediction_id必须为ObjectId对象或其对应的字符串！")
        for f in os.listdir(kegg_path):
            if re.search(r"kegg.enzyme.profile.xls$", f):
                enzyme_path = os.path.join(kegg_path, f)
            if re.search(r"kegg.pathway.profile.xls$", f):
                pathway_path = os.path.join(kegg_path, f)
            if re.search(r"predictions_ko.xls$", f):
                ko_path = os.path.join(kegg_path, f)
        if not os.path.exists(enzyme_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(enzyme_path))
        if not os.path.exists(pathway_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(pathway_path))
        if not os.path.exists(ko_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(ko_path))
        if not os.path.exists(table_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(table_path))
        data_list = []
        with open(enzyme_path, "rb") as en, open(table_path, "rb") as t:
            lines = en.readlines()
            line1 = t.readline().strip().split('\t')
            for line in lines[1:]:
                line = line.strip().split("\t")
                data = [
                    ("prediction_id", prediction_id),
                    ("type", "enzyme"),
                    ("enzyme", line[0]),
                    ("definition", line[-1]),
                ]
                for i in range(1, len(line1)):
                    data += [
                        (line1[i], int(line[i]))
                    ]
                data = SON(data)
                data_list.append(data)
        with open(pathway_path, "rb") as pa, open(table_path, "rb") as t:
            lines = pa.readlines()
            line1 = t.readline().strip().split('\t')
            for line in lines[1:]:
                line = line.strip().split("\t")
                ko_id = line[0]
                for f in os.listdir(maps_path):
                    m = re.match(r"(ko.*).pdf", f)
                    if m:
                        ko = m.group(1)
                    if ko == ko_id:
                        graph_dir = maps_path + '/' + ko + '.pdf'
                        fs = gridfs.GridFS(self.db)
                        gra = fs.put(open(graph_dir, 'rb'))
                data = [
                    ("prediction_id", prediction_id),
                    ("type", "pathway"),
                    ("pathway", line[0]),
                    ("definition", line[-2]),
                    ("pathwaymap", line[-1]),
                    ("graph_id", gra),
                ]
                for i in range(1, len(line1)):
                    data += [
                        (line1[i], int(line[i]))
                    ]
                data = SON(data)
                data_list.append(data)
        with open(ko_path, "rb") as ko, open(table_path, "rb") as t:
            lines = ko.readlines()
            line1 = t.readline().strip().split('\t')
            for line in lines[1:]:
                line = line.strip().split("\t")
                data = [
                    ("prediction_id", prediction_id),
                    ("type", "ko"),
                    ("ko_id", line[0]),
                ]
                for i in range(1, len(line1)):
                    data += [
                        (line1[i], int(line[i]))
                    ]
                data = SON(data)
                data_list.append(data)
        for j in [1, 2, 3]:
            level_path = kegg_path + "/predictions_ko.L" + str(j) + ".xls"
            if os.path.exists(level_path):
                with open(level_path, "rb") as le, open(table_path, "rb") as t:
                    lines = le.readlines()
                    line1 = t.readline().strip().split('\t')
                    for line in lines[1:]:
                        line = line.strip().split("\t")
                        data = [
                            ("prediction_id", prediction_id),
                            ("type", "level"),
                            ("level", j),
                            ("category", line[0]),
                        ]
                        for i in range(1, len(line1)):
                            data += [
                                (line1[i], int(line[i]))
                            ]
                        data = SON(data)
                        data_list.append(data)
            else:
                raise Exception("kegg功能预测的结果文件中level丰度表不存在，请检查！")
        try:
            collection = self.db["sg_kegg_abundance_stat"]
            collection.insert_many(data_list)
        except Exception as e:
            self.bind_object.logger.error("导入kegg 16s 功能预测%s、%s、%s、%s信息出错:%s" % (enzyme_path, pathway_path, ko_path, level_path, e))
        else:
            self.bind_object.logger.info("导入kegg 16s 功能预测%s、%s、%s、%s信息成功!" % (enzyme_path, pathway_path, ko_path, level_path))
