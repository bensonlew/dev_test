# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
from biocluster.api.database.base import Base, report_check
import re
from bson.objectid import ObjectId
from types import StringTypes
from bson.son import SON
import gridfs
import datetime


class StatTest(Base):
    def __init__(self, bind_object):
        super(StatTest, self).__init__(bind_object)
        self._db_name = "sanger"

    @report_check
    def add_species_difference_check_detail(self, file_path, level=None, check_type=None, params=None, table_id=None, group_id=None, from_otu_table=None, name=None, major=False):
        if major:
            table_id = self.create_species_difference_check(level, check_type, params, group_id,  from_otu_table, name)
        else:
            if table_id is None:
                raise Exception("major为False时需提供table_id!")
            if not isinstance(table_id, ObjectId):
                if isinstance(table_id, StringTypes):
                    table_id = ObjectId(table_id)
                else:
                    raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            l = r.readline().strip('\n')
            group_list = re.findall(r'mean\((.*?)\)', l)
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                length = len(line_data)
                i = 1
                for name in group_list:
                    data = [("species_check_id", table_id),("species_name", line_data[0]),("qvalue", float(line_data[length-1])),("pvalue", float(line_data[length-2]))]
                    data.append(("category_name", name))
                    data.append(("mean", float(line_data[i])))
                    data.append(("sd", float(line_data[i+1])))
                    i += 2
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_species_difference_check_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
    
    @report_check
    def add_twosample_species_difference_check_detail(self, file_path, level=None, check_type=None, params=None, table_id=None, group_id=None, from_otu_table=None, name=None, major=False):
        if major:
            table_id = self.create_species_difference_check(level, check_type, params, group_id,  from_otu_table, name)
        else:
            if table_id is None:
                raise Exception("major为False时需提供table_id!")
            if not isinstance(table_id, ObjectId):
                if isinstance(table_id, StringTypes):
                    table_id = ObjectId(table_id)
                else:
                    raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            l = r.readline().strip('\n')
            s1 = re.search(r'propotion\((.*)\)\W+propotion\((.*)\)', l)
            sample = s1.groups(0)
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                data = [("species_check_id", table_id), ("species_name", line_data[0]), ("qvalue", float(line_data[4])),
                        ("pvalue", float(line_data[3])), ("specimen_name", sample[0]), ("propotion", float(line_data[1]))]
                data_son = SON(data)
                data_list.append(data_son)
                data1 = [("species_check_id", table_id), ("species_name", line_data[0]), ("qvalue", float(line_data[4])),
                         ("pvalue", float(line_data[3])), ("specimen_name", sample[1]), ("propotion", float(line_data[2]))]
                data_son1 = SON(data1)
                data_list.append(data_son1)
        try:
            collection = self.db["sg_species_difference_check_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)

    @report_check
    def add_species_difference_check_boxplot(self, file_path, table_id):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            l = r.readline().strip('\n')
            group_list = re.findall(r'min\((.*?)\)', l)
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                i = 1
                for name in group_list:
                    data = [("species_check_id", table_id), ("species_name", line_data[0])]
                    data.append(("category_name", name))
                    data.append(("min", float(line_data[i])))
                    data.append(("q1", float(line_data[i+1])))
                    data.append(("median", float(line_data[i+2])))
                    data.append(("q3", float(line_data[i+3])))
                    data.append(("max", float(line_data[i+4])))
                    i += 5
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_species_difference_check_boxplot"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    @report_check
    def add_species_difference_check_ci_plot(self, file_path, table_id):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            l = r.readline()
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                data = [("species_check_id", table_id), ("species_name", line_data[0]), ("effectsize", float(line_data[1])),
                        float(("lower_ci", line_data[2])), float(("upper_ci", line_data[3]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["sg_species_difference_check_ci_plot"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    @report_check
    def add_mulgroup_species_difference_check_ci_plot(self, file_path, table_id):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        groups = file_path.split('/')
        filename = groups[len(groups) - 1].split('_')
        compare_group = filename[len(filename) - 1]
        data_list = []
        with open(file_path, 'rb') as r:
            l = r.readline()
            while True:
                line = r.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                data = [("species_check_id", table_id), ("species_name", line_data[0]), float(("effectsize", line_data[1])),
                        float(("lower_ci", line_data[2])), float(("upper_ci", line_data[3])), float(("post_hoc_pvalue", line_data[4])),
                        ("compare_category", compare_group.replace('.xls',''))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.db["sg_species_difference_check_ci_plot"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    @report_check
    def add_species_difference_lefse_detail(self, file_path, params=None, group_id=None,  from_otu_table=None, name=None, table_id=None, major=False):
        if major:
            table_id = self.create_species_difference_lefse(params, group_id, from_otu_table, name)
        else:
            if table_id is None:
                raise Exception("major为False时需提供table_id!")
            if not isinstance(table_id, ObjectId):
                if isinstance(table_id, StringTypes):
                    table_id = ObjectId(table_id)
                else:
                    raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            i = 0
            for line in r:
                if i == 0:
                    i = 1
                else:
                    line = line.strip('\n')
                    line_data = line.split('\t')
                    data = [("species_lefse_id", table_id), ("species_name", line_data[0]),
                            ("category_name", line_data[2]), ("median", float(line_data[1])), ("lda", line_data[3]),
                            ("pvalue", line_data[4])]
                    data_son = SON(data)
                    data_list.append(data_son)
        try:
            collection = self.db["sg_species_difference_lefse_detail"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list

    @report_check
    def update_species_difference_lefse(self, lda_png_path, lda_cladogram_path, table_id):
        collection = self.db["sg_species_difference_lefse"]
        fs = gridfs.GridFS(self.db)
        ldaid = fs.put(open(lda_png_path, 'r'))
        cladogramid = fs.put(open(lda_cladogram_path, 'r'))
        try:
            collection.update({"_id": ObjectId(table_id)}, {"$set": {"lda_png_id": ldaid, "lda_cladogram_id": cladogramid}})
        except Exception, e:
            self.bind_object.logger.error("导入%s和%s信息出错:%s" % (lda_png_path, lda_cladogram_path, e))
        else:
            self.bind_object.logger.info("导入%s和%s信息成功!" % (lda_png_path, lda_cladogram_path))

    def create_species_difference_check(self, level, check_type, params, group_id=None,  from_otu_table=None, name=None):
        if from_otu_table is not None and not isinstance(from_otu_table, ObjectId):
            if isinstance(from_otu_table, StringTypes):
                from_otu_table = ObjectId(from_otu_table)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        if group_id is not None and not isinstance(group_id, ObjectId):
            if isinstance(group_id, StringTypes):
                group_id = ObjectId(group_id)
            else:
                raise Exception("group_detail必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_otu"]
        result = collection.find_one({"_id": from_otu_table})
        project_sn = result['project_sn']
        task_id = result['task_id']
        if check_type == 'tow_sample':
             insert_data = {
                "type": check_type,
                "project_sn": project_sn,
                "task_id": task_id,
                "name": name if name else "组间差异统计表格",
                "level_id": int(level),
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
             }
        else:
            insert_data = {
                "type": check_type,
                "project_sn": project_sn,
                "task_id": task_id,
                "otu_id": from_otu_table,
                "group_id": group_id,
                "name": name if name else "组间差异统计表格",
                "level_id": int(level),
                "params": params,
                "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        collection = self.db["sg_species_difference_check"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    def create_species_difference_lefse(self, params, group_id=None,  from_otu_table=None, name=None):
        if from_otu_table is not None and not isinstance(from_otu_table, ObjectId):
            if isinstance(from_otu_table, StringTypes):
                from_otu_table = ObjectId(from_otu_table)
            else:
                raise Exception("from_otu_table必须为ObjectId对象或其对应的字符串!")
        if group_id is not None and not isinstance(group_id, ObjectId):
            if isinstance(group_id, StringTypes):
                group_id = ObjectId(group_id)
            else:
                raise Exception("group_detail必须为ObjectId对象或其对应的字符串!")
        collection = self.db["sg_otu"]
        result = collection.find_one({"_id": from_otu_table})
        project_sn = result['project_sn']
        task_id = result['task_id']
        insert_data = {
            "project_sn": project_sn,
            "task_id": task_id,
            "otu_id": from_otu_table,
            "group_id": group_id,
            "name": name if name else "lefse分析LDA值表",
            "params": params,
            "lda_cladogram_id": "",
            "lda_png_id": "",
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        collection = self.db["sg_species_difference_lefse"]
        inserted_id = collection.insert_one(insert_data).inserted_id
        return inserted_id

    @report_check
    def update_species_difference_check(self, errorbar_path, table_id):
        collection = self.db["sg_species_difference_check"]
        fs = gridfs.GridFS(self.db)
        errorbar_id = fs.put(open(errorbar_path, 'r'))
        try:
            collection.update({"_id": ObjectId(table_id)}, {"$set": {"errorbar_png": errorbar_id}})
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (errorbar_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % (errorbar_path))



