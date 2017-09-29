# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'
# last_modify:20170922
from biocluster.api.database.base import Base, report_check
import os
import re
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId


class DataStat(Base):
    def __init__(self, bind_object):
        super(DataStat, self).__init__(bind_object)
        self._db_name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_data_stat(self, data_type, stat_path, dir_path, raw_data_stat_id):
        task_id = self.bind_object.sheet.id
        project_sn = self.bind_object.sheet.project_sn
        created_ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = {
            'project_sn': project_sn,
            'task_id': task_id,
            'desc': '数据质控统计主表',
            'created_ts': created_ts,
            'params': 'null',
            'status': 'end',
            'type': data_type,
        }
        collection = self.db['data_stat']
        # 将主表名称写在这里
        data_stat_id = collection.insert_one(insert_data).inserted_id
        # 将导表数据通过insert_one函数导入数据库，将此条记录生成的_id作为返回值，给detail表做输入参数
        if data_type in ["raw"]:
            self.add_data_stat_detail(stat_path, data_stat_id, data_type, raw_data_stat_id=None)
            self.add_specimen_graphic(dir_path, task_id)
        if data_type in ["clean", "optimised"]:
            self.add_data_stat_detail(stat_path, data_stat_id, data_type, raw_data_stat_id)
        return data_stat_id

    @report_check
    def add_data_stat_detail(self, stat_path, data_stat_id, data_type, raw_data_stat_id=None):
        if not isinstance(data_stat_id, ObjectId):
            if isinstance(data_stat_id, types.StringTypes):
                data_stat_id = ObjectId(data_stat_id)
            else:
                raise Exception('data_stat_id必须为ObjectId对象或其对应的字符串！')
        if not os.path.exists(stat_path):
            raise Exception('stat_path所指定的路径不存在，请检查！')
        data_list = []  # 存入表格中的信息，然后用insert_many批量导入
        if data_type in ["clean", "optimised"]:
            mydb = self.db['data_stat_detail']
            raws = mydb.find({"data_stat_id": ObjectId(raw_data_stat_id)})
            # raws= mydb.find({"data_stat_id": ObjectId("59cde2d3a4e1af116ca21c32")})
            if raws is None:
                raise Exception('没有找到样品集数据')
            specimen = {}
            for raw in raws:
                specimen[raw['specimen_name']] = {"raw_read_num": raw['raw_read_num']}
                specimen[raw['specimen_name']]["raw_base"] = raw['raw_base']
        with open(stat_path, 'rb') as f:
            lines = f.readlines()
            for line in lines[1:]:  # 从第二行记录信息，因为第一行通常是表头文件，忽略掉
                line = line.strip().split('\t')
                data = {
                    "data_stat_id": data_stat_id,
                    "specimen_name": line[0],
                }
                if data_type == "raw":
                    data['specimen_source'] = line[1]
                    data['insert_size'] = int(line[2])
                    data['raw_read_len'] = int(line[3])
                    data['raw_read_num'] = int(line[4])
                    data['raw_base'] = int(line[5])
                if data_type == "clean":
                    read_num = specimen[line[0]]["raw_read_num"]
                    base = specimen[line[0]]["raw_base"]
                    clean_ratio = float(line[1]) / read_num
                    clean_base_ratio = float(line[2]) / base
                    data['clean_read_num'] = int(line[1])
                    data['clean_base'] = int(line[2])
                    data['clean_ratio'] = clean_ratio
                    data['clean_base_ratio'] = clean_base_ratio
                if data_type == "optimised":
                    read_num = specimen[line[0]]["raw_read_num"]
                    base = specimen[line[0]]["raw_base"]
                    opt_ratio = float(line[1]) / read_num
                    opt_base_ratio = float(line[2]) / base
                    data['opt_read_num'] = int(line[1])
                    data['opt_base'] = int(line[2])
                    data['opt_ratio'] = opt_ratio
                    data['opt_base_ratio'] = opt_base_ratio
                data_list.append(data)
        try:
            collection = self.db['data_stat_detail']
            collection.insert_many(data_list)  # 用insert_many批量导入数据库，insert_one一次只能导入一条记录
        except Exception, e:
            self.bind_object.logger.error("导入%s信息出错:%s" % (stat_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % stat_path)

    @report_check
    def add_specimen_graphic(self, dir_path, task_id):
        data_list = []
        filelist = os.listdir(dir_path.rstrip('/'))
        for i in filelist:
            line = i.strip().split('.', 1)
            specimen = line[0]
            if re.search(r'1\.(fastq|fq)\.', line[1]):
                reads_direct = "left"
            elif re.search(r'2\.(fastq|fq)\.', line[1]):
                reads_direct = "right"
            else:
                raise Exception(line[1])
            data_list = []
            specimen_graphic = dir_path.rstrip('/') + '/' + i
            with open(specimen_graphic, 'rb') as f:
                lines = f.readlines()
                for line in lines[1:]:
                    line = line.strip().split('\t')
                    data = {
                        "task_id": task_id,
                        "specimen_name": specimen,
                        "type": reads_direct,
                        "column": line[0],
                        "min": line[2],
                        "max": line[3],
                        "q1": line[6],
                        "median": line[7],
                        "q3": line[8],
                        "A": line[12],
                        "C": line[13],
                        "G": line[14],
                        "T": line[15],
                        "N": line[16],
                    }
                    data_list.append(data)
            try:
                collection = self.db['specimen_graphic']
                collection.insert_many(data_list)
            except Exception, e:
                self.bind_object.logger.error("导入%s信息出错:%s" % (specimen_graphic, e))
            else:
                self.bind_object.logger.info("导入%s信息成功!" % specimen_graphic)
