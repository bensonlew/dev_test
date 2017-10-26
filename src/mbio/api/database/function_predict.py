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
# from biocluster.config import Config


class FunctionPredict(Base):
    def __init__(self, bind_object):
        super(FunctionPredict, self).__init__(bind_object)
        self._project_type = 'meta'
        # self._db_name = Config().MONGODB

    @report_check
    def add_function_prediction(self, name=None, params=None, otu_id=0, sample_path=None):
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
        with open(sample_path, "rb") as t:
            line1 = t.readline().strip().split('\t')
            for i in range(1, len(line1)):
                specimen.append(line1[i])
        insert_data = {
            "project_sn": project_sn,
            "task_id": task_id,
            "name": name if name else "16s_function_prediction_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")),
            "params": params,
            "status": "end",
            "desc": "16s功能预测主表",
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "specimen": specimen
        }
        collection = self.db["sg_16s"]
        prediction_id = collection.insert_one(insert_data).inserted_id
        return prediction_id

    @report_check
    def update_specimen(self, sample_path, prediction_id):
        if not isinstance(prediction_id, ObjectId):
            if isinstance(prediction_id, types.StringTypes):
                prediction_id = ObjectId(prediction_id)
            else:
                raise Exception("prediction_id必须为ObjectId对象或其对应的字符串！")
        collection = self.db["sg_16s"]
        result = collection.find_one({"_id": prediction_id})
        if not result:
            raise Exception("无法根据传入的_id:{}在sg_16s表里找到相应的记录".format(str(prediction_id)))
        specimen = []
        with open(sample_path, "rb") as t:
            line1 = t.readline().strip().split('\t')
            for i in range(1, len(line1)):
                specimen.append(line1[i])
        collection.update({'_id': ObjectId(prediction_id)}, {'$set': {'specimen': specimen}})

    @report_check
    def add_cog_function(self, prediction_id, sample_path, function_path):
        if not isinstance(prediction_id, ObjectId):
            if isinstance(prediction_id, types.StringTypes):
                prediction_id = ObjectId(prediction_id)
            else:
                raise Exception("prediction_id必须为ObjectId对象或其对应的字符串！")
        if not os.path.exists(sample_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(sample_path))
        if not os.path.exists(function_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(function_path))
        data_list = []
        with open(sample_path, "rb") as s, open(function_path, "rb") as t:
            lines = t.readlines()
            line1 = s.readline().strip().split('\t')
            for line in lines[1:]:
                line = line.strip().split("\t")
                data = [
                    ("prediction_id", prediction_id),
                    ("catergory", line[0]),
                    ("description", line[-1]),
                ]
                mylist = []
                summary = 0
                for i in range(1, len(line1)):
                    data += [
                        (line1[i], int(line[i]))
                    ]
                    summary += int(line[i])
                    mylist.append(int(line[i]))
                box = self.get_box_new(mylist)
                data.append(("box", box))
                data.append(("calculation", summary))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_16s_cog_function"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入cog功能预测%s、%s信息出错:%s" % (sample_path, function_path, e))
        else:
            self.bind_object.logger.info("导入cog功能预测%s、%s信息成功!" % (sample_path, function_path))

    @report_check
    def add_cog_specimen(self, prediction_id, group_method, sample_path, table_path, predict_path):
        if not isinstance(prediction_id, ObjectId):
            if isinstance(prediction_id, types.StringTypes):
                prediction_id = ObjectId(prediction_id)
            else:
                raise Exception("prediction_id必须为ObjectId对象或其对应的字符串！")
        if not os.path.exists(sample_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(sample_path))
        if not os.path.exists(table_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(table_path))
        if not os.path.exists(predict_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(predict_path))
        data_list = []
        with open(sample_path, "rb") as f, open(table_path, "rb") as t, open(predict_path, "rb") as c:
            lines = t.readlines()
            lines2 = c.readlines()
            line1 = f.readline().strip().split('\t')
            for i in range(1, len(lines)):
                line = lines[i].strip().split("\t")
                cate = lines2[i].strip().split("\t")
                data = [
                    ("prediction_id", prediction_id),
                    ("cog_id", line[0]),
                    ("description", line[-1]),
                    ("category", cate[-1])
                ]
                mylist = []
                calculation, summary = 0, 0
                for i in range(1, len(line1)):
                    data += [
                        (line1[i], int(line[i]))
                    ]
                    summary += int(line[i])
                    mylist.append(int(line[i]))
                if group_method == "middle":
                    my = sorted(mylist)
                    if (len(my) % 2) == 1:
                        calculation = my[len(my)/2]
                    else:
                        calculation = float(my[len(my)/2] + my[len(my)/2 - 1]) / 2
                else:
                    calculation = summary
                box = self.get_box_new(mylist)
                data.append(("box", box))
                data.append(("calculation", calculation))
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_16s_cog_specimen"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入cog样品功能预测%s、%s信息出错:%s" % (sample_path, table_path, e))
        else:
            self.bind_object.logger.info("导入cog样品功能预测%s、%s信息成功!" % (sample_path, table_path))

    @report_check
    def add_kegg_specimen(self, prediction_id, kegg_path, maps_path, sample_path):
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
        if not os.path.exists(sample_path):
            raise Exception("{}所指定的路径不存在，请检查！".format(sample_path))
        data_list = []
        with open(enzyme_path, "rb") as en, open(sample_path, "rb") as t:
            lines = en.readlines()
            line1 = t.readline().strip().split('\t')
            for line in lines[1:]:
                line = line.strip().split("\t")
                data = [
                    ("prediction_id", prediction_id),
                    ("type", "enzyme"),
                    ("name", line[0]),
                    ("definition", line[-1]),
                ]
                for i in range(1, len(line1)):
                    data += [
                        (line1[i], int(line[i]))
                    ]
                data = SON(data)
                data_list.append(data)
        with open(pathway_path, "rb") as pa, open(sample_path, "rb") as t:
            lines = pa.readlines()
            line1 = t.readline().strip().split('\t')
            for line in lines[1:]:
                line = line.strip().split("\t")
                ko_id = line[0]
                for f in os.listdir(maps_path):
                    m = re.match(r"(ko.*).png", f)
                    if m:
                        ko = m.group(1)
                    if ko == ko_id:
                        graph_dir = maps_path + '/' + ko + '.png'
                        if os.path.exists(graph_dir):
                            fs = gridfs.GridFS(self.db)
                            gra = fs.put(open(graph_dir, 'rb'))
                data = [
                    ("prediction_id", prediction_id),
                    ("type", "pathway"),
                    ("name", line[0]),
                    ("definition", line[-2]),
                    ("graph_id", gra),
                ]
                for i in range(1, len(line1)):
                    data += [
                        (line1[i], int(line[i]))
                    ]
                data = SON(data)
                data_list.append(data)
        with open(ko_path, "rb") as ko, open(sample_path, "rb") as t:
            lines = ko.readlines()
            line1 = t.readline().strip().split('\t')
            for line in lines[1:]:
                line = line.strip().split("\t")
                data = [
                    ("prediction_id", prediction_id),
                    ("type", "ko"),
                    ("name", line[0]),
                ]
                for i in range(1, len(line1)):
                    data += [
                        (line1[i], int(line[i]))
                    ]
                data = SON(data)
                data_list.append(data)
        try:
            collection = self.db["sg_16s_kegg_specimen"]
            collection.insert_many(data_list)
        except Exception as e:
            self.bind_object.logger.error("导入kegg 16s 功能预测%s、%s、%s信息出错:%s" % (enzyme_path, pathway_path, ko_path, e))
        else:
            self.bind_object.logger.info("导入kegg 16s 功能预测%s、%s、%s信息成功!" % (enzyme_path, pathway_path, ko_path))

    @report_check
    def add_kegg_level(self, prediction_id, kegg_path, sample_path):
        if not isinstance(prediction_id, ObjectId):
            if isinstance(prediction_id, types.StringTypes):
                prediction_id = ObjectId(prediction_id)
            else:
                raise Exception("prediction_id必须为ObjectId对象或其对应的字符串！")
        data_list = []
        for j in [1, 2, 3]:
            level_path = kegg_path + "/predictions_ko.L" + str(j) + ".xls"
            if os.path.exists(level_path):
                with open(level_path, "rb") as le, open(sample_path, "rb") as t:
                    lines = le.readlines()
                    line1 = t.readline().strip().split('\t')
                    for line in lines[1:]:
                        line = line.strip().split("\t")
                        data = [
                            ("prediction_id", prediction_id),
                            ("type", "level"),
                            ("level", j),
                            ("name", line[0]),
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
            collection = self.db["sg_16s_kegg_level"]
            collection.insert_many(data_list)
        except Exception as e:
            self.bind_object.logger.error("导入kegg 16s 功能预测%s信息出错:%s" % (level_path, e))
        else:
            self.bind_object.logger.info("导入kegg 16s 功能预测%s信息成功!" % (level_path))

    def get_box_new(self, mylist):
        mylist = sorted(mylist)
        length = len(mylist)
        filter_list = []
        out = []
        if length > 3:
            q1_index = 0.25 * (length + 1)
            q2_index = 0.5 * (length + 1)
            q3_index = 0.75 * (length + 1)
            q1_index_int = int(q1_index)
            q2_index_int = int(q2_index)
            q3_index_int = int(q3_index)
            q1 = mylist[q1_index_int - 1] + (mylist[q1_index_int] - mylist[q1_index_int - 1]) * (q1_index - q1_index_int)
            q3 = mylist[q3_index_int - 1] + (mylist[q3_index_int] - mylist[q3_index_int - 1]) * (q3_index - q3_index_int)
            q2 = mylist[q2_index_int - 1] + (mylist[q2_index_int] - mylist[q2_index_int - 1]) * (q2_index - q2_index_int)
            qd = q3 - q1
            max_limit = 1.5 * qd + q3
            min_limit = q1 - 1.5 * qd
            new_list = []
            for i in mylist:
                if i >= min_limit and i <= max_limit:
                    new_list.append(i)
                else:
                    filter_list.append(i)
            max_box = new_list[-1]
            min_box = new_list[0]
        elif(length == 3):
            max_box = mylist[2]
            min_box = mylist[0]
            q3 = float(mylist[1] + mylist[2]) / 2
            q2 = mylist[1]
            q1 = float(mylist[1] + mylist[0]) / 2
        elif(length == 2):
            max_box = mylist[1]
            min_box = mylist[0]
            q2 = float(mylist[1] + mylist[0]) / 2
            q3 = (mylist[1] + q2) / 2
            q1 = (q2 + mylist[0]) / 2
        elif(length == 1):
            max_box = mylist[0]
            min_box = mylist[0]
            q2 = mylist[0]
            q3 = mylist[0]
            q1 = mylist[0]
        out.append(min_box)
        out.append(q1)
        out.append(q2)
        out.append(q3)
        out.append(max_box)
        out.append(filter_list)
        return out
