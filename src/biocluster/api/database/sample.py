# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from .base import Base
import os
import re


class Sample(Base):
    def __init__(self, bind_object):
        super(Sample, self).__init__(bind_object)
        self._db_name = "sanger"

    def add_samples_info(self, file_path):
        if self.bind_object.IMPORT_REPORT_DATA is True:
            self.bind_object.logger.debug("非web客户端调用，跳过导入样品信息!")
            return
        if not os.path.isfile(file_path):
            raise Exception("文件%s不存在!" % file_path)
        data_list = []
        with open(file_path, 'r') as f:
            l = f.readline()
            if not re.match(r"sample\s+reads\s+bases\s+avg\s+min\s+max",l):
                raise Exception("文件%s格式不正确，请选择正确的样品信息文件" % file_path)
            while True:
                line = f.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                data = {
                  "project_sn": self.bind_object.sheet.project_sn,
                  "task_id": self.bind_object.sheet.id,
                  "specimen_name": line_data[0],
                  "read_number": line_data[1],
                  "base_number": line_data[2],
                  "average_length": line_data[3],
                  "min_length": line_data[4],
                  "max_length": line_data[5]
                }
                data_list.append(data)
        try:
            collection = self.db["sg_specimen"]
            result = collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入样品信息数据出错:%s" % e)
        else:
            self.bind_object.logger.info("导入样品信息数据成功:%s" % result.inserted_ids)

    def add_base_info(self, sample_name, file_path):
        if self.bind_object.IMPORT_REPORT_DATA is True:
            self.bind_object.logger.debug("非web客户端调用，跳过导入样品%s碱基统计信息!" % sample_name)
            return
        if not os.path.isfile(file_path):
            raise Exception("文件%s不存在!" % file_path)
        collection = self.db["sg_specimen"]
        result = collection.find_one({"specimen_name": sample_name})
        if result:
            specimen_id = result["_id"]
        else:
            raise Exception("没有找到对应的样品%s信息，请先导入样品信息表!" % sample_name)
        data_list = []
        with open(file_path, 'r') as f:
            l = f.readline()
            if not re.match(r"column\tcount\tmin\tmax\tsum\tmean\tQ1\tmed\tQ3\tIQR\tlW\trW\tA_Count\tC_Count\tG_Count\tT_Count\tN_Count\tMax_count",l):
                raise Exception("文件%s格式不正确，请选择正确的碱基统计文件" % file_path)
            while True:
                line = f.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                data = {
                  "project_sn": self.bind_object.sheet.project_sn,
                  "task_id": self.bind_object.sheet.id,
                  "specimen_id": specimen_id,
                  "column": line_data[0],
                  "min": line_data[2],
                  "max": line_data[3],
                  "q1": line_data[6],
                  "q3": line_data[8],
                  "median": line_data[7],
                  "average": line_data[5]
                }
                data_list.append(data)
        try:
            collection = self.db["sg_specimen_sequence"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入样品%s的碱基统计信息出错:%s" % (sample_name, e))
        else:
            self.bind_object.logger.info("导入样品%s的碱基统计信息成功" % sample_name)

    def add_reads_len_info(self, step_length, file_path):
        if self.bind_object.IMPORT_REPORT_DATA is True:
            self.bind_object.logger.debug("非web客户端调用，跳过导入%s步长序列长度统计!" % step_length)
            return
        if not os.path.isfile(file_path):
            raise Exception("文件%s不存在!" % file_path)

        data_list = []
        with open(file_path, 'r') as f:
            l = f.readline()
            if not re.match(r"sample\t", l):
                raise Exception("文件%s格式不正确，请选择正确的碱基统计文件" % file_path)
            else:
                length_list = l.strip("\n").split("\t")
                length_list.pop(0)
            while True:
                line = f.readline().strip('\n')
                if not line:
                    break
                line_data = line.split("\t")
                collection = self.db["sg_specimen"]
                result = collection.find_one({"specimen_name": line_data[0]})
                if result:
                    specimen_id = result["_id"]
                else:
                    raise Exception("没有找到对应的样品%s信息，请先导入样品信息表!" % line_data[0])
                step_data = {}
                i = 0
                for step in length_list:
                    i += 1
                    step_data[step] = line_data[i]
                data = {
                  "project_sn": self.bind_object.sheet.project_sn,
                  "task_id": self.bind_object.sheet.id,
                  "specimen_id": specimen_id,
                  "step": step_length,
                  "value": step_data
                }
                data_list.append(data)
        try:
            collection = self.db["sg_specimen_step"]
            collection.insert_many(data_list)
        except Exception, e:
            self.bind_object.logger.error("导入步%s的步长序列长度统计出错:%s" % (step_length, e))
        else:
            self.bind_object.logger.info("导入步%s的步长序列长度统计成功" % step_length)
