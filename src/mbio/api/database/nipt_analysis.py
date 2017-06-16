# -*- coding: utf-8 -*-
# __author__ = 'xuanhongdong'
# last_modify:20170519
import datetime
import re

from biocluster.api.database.base import Base, report_check
from bson.objectid import ObjectId
from types import StringTypes
from bson.son import SON
from biocluster.config import Config
import xlrd
import os
from mainapp.libs.param_pack import param_pack


class NiptAnalysis(Base):
    def __init__(self, bind_object):
        super(NiptAnalysis, self).__init__(bind_object)
        self.mongo_client = Config().mongo_client
        self.database = self.mongo_client[Config().MONGODB+'_nipt']  # 结果库
        self.mongo_client_ref = Config().biodb_mongo_client
        self.database_ref = self.mongo_client_ref['sanger_nipt_ref']  # 参考库

    # @report_check
    def add_zz_result(self, file_path, table_id=None):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或者其对应的字符串！")
        data_list = []
        with open(file_path, "rb") as r:
            data1 = r.readlines()[1:]
            for line1 in data1:
                temp1 = line1.rstrip().split("\t")
                data = [("interaction_id", table_id), ("sample_id", str(temp1[0])), ("zz", eval(temp1[1]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.database["sg_interaction_zzresult"]
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list, table_id

    def update_main(self,main_id,file_path):
        with open(file_path, "rb") as r:
            data1 = r.readlines()[1:]
            for line1 in data1:
                temp1 = line1.rstrip().split("\t")
                insert = {
                    "zz":eval(temp1[1])
                }

        try:
            collection = self.database["sg_main"]
            collection.update({"_id": main_id}, {'$set': insert})
        except Exception, e:
            raise Exception("更新主表出错：{}".format(e))
        else:
            self.bind_object.logger.info("更新主表成功!")

    def get_id(self,name):
        collection = self.database["sg_main"]
        temp = collection.find_one({"sample_id": name})
        collection_inter = self.database["sg_interaction"]
        temp1 = collection_inter.find_one({"main_id": temp[u'_id']})
        return temp[u'_id'],temp1[u'id']

    # @report_check
    def add_z_result(self, file_path, table_id=None):
        if not isinstance(table_id, ObjectId):
            if isinstance(table_id, StringTypes):
                table_id = ObjectId(table_id)
            else:
                raise Exception("table_id必须为ObjectId对象或其对应的字符串!")
        data_list = []
        with open(file_path, 'rb') as r:
            data1 = r.readlines()[1:]
            for line in data1:
                temp = line.rstrip().split("\t")
                data = [("interaction_id", table_id), ("sample_id", str(temp[0])), ("chr", int(temp[1])),
                        ("cn", eval(temp[2])), ("bin", int(temp[3])), ("n", int(temp[4])), ("sd", eval(temp[5])),
                        ("mean", eval(temp[6])), ("z", eval(temp[7]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.database["sg_interaction_zresult"]
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息成功!" % file_path)
        return data_list


    def add_bed_file(self, file_path):
        """
        用于添加nipt分析shell部分生成的bed文件到mongo表中
        """
        data_list = []
        with open(file_path, 'rb') as r:
            data1 = r.readlines()
            for line in data1:
                temp = line.rstrip().split("\t")
                data = [("chr", str(temp[0])),
                        ("start", str(temp[1])), ("end", str(temp[2])), ("gc", str(temp[3])), ("map", str(temp[4])),
                        ("pn", str(temp[5])), ("reads", str(temp[6])), ("sample_id", str(temp[7]))]
                data_son = SON(data)
                data_list.append(data_son)
        try:
            collection = self.database_ref["sg_bed"]
            collection.insert_many(data_list)
        except Exception, e:
            raise Exception("导入%s信息出错:%s" % (file_path, e))
        else:
            self.bind_object.logger.info("导入%s信息到sg_nipt_bed成功!" % file_path)
        return True

    def nipt_customer(self, file):
        bk = xlrd.open_workbook(file)
        sh = bk.sheet_by_name(u'\u65e0\u521b\u4ea7\u7b5b')
        nrows = sh.nrows

        insert = list()
        # 获取各行数据
        for i in range(4, nrows):
            row_data = sh.row_values(i)
            if i == 4:
                report_num_index = row_data.index(
                    u'\u9879\u76ee\u7f16\u53f7\uff08\u62a5\u544a\u7f16\u53f7\uff09')  # 报告编号
                sample_date_index = row_data.index(u'\u91c7\u6837\u65e5\u671f')  # 采样日期
                patient_name_index = row_data.index(u'\u60a3\u8005\u59d3\u540d\u6837\u672c\u540d\u79f0')  # 患者姓名
                accpeted_date_index = row_data.index(u'\u6536\u6837\u65e5\u671f')  # 收样日期
                number_index = row_data.index(u'\u7f8e\u5409\u6761\u5f62\u7801')  # 样本编号
                register_number_index = row_data.index(u'\u4f4f\u9662/\u95e8\u8bca\u53f7')  # 住院号
                gestation_index = row_data.index(u'\u5b55\u4ea7\u53f2')  # 孕产史
                final_period_index = row_data.index(u'\u672b\u6b21\u6708\u7ecf')  # 末次月经
                gestation_week_index = row_data.index(u'\u5b55\u5468')  # 孕周
                pregnancy_index = row_data.index(u'\u5355\u80ce/\u53cc\u80ce')  # 单双胎
                IVFET_index = row_data.index(u'IVF-ET\u598a\u5a20')  # IVF-ET妊娠
                hospital_index = row_data.index(u'\u9001\u68c0\u5355\u4f4d/\u533b\u9662')  # 送检单位、医院
                doctor_index = row_data.index(u'\u9001\u68c0\u533b\u751f')  # 送检医生
                tel_index = row_data.index(u'\u60a3\u8005\u8054\u7cfb\u7535\u8bdd')  # 患者联系方式
                status_index = row_data.index(u'\u6807\u672c\u72b6\u6001\u5f02\u5e38')  # 标本状态异常
                age_index = row_data.index(u'\u5e74\u9f84')  # 年龄
                type_index = row_data.index(u'\u6807\u672c\u7c7b\u578b')  # 样本类型
            else:
                para_list = []
                report_num = row_data[report_num_index]
                para_list.append(report_num)
                sample_date = row_data[sample_date_index]
                # if sample_date == 'Nf':
                #     para_list.append(sample_date)
                # elif sample_date == '/':
                #     para_list.append(sample_date)
                if type(sample_date) == float:
                    sample_date_tuple = xlrd.xldate_as_tuple(sample_date, 0)
                    para_list.append(
                        str(sample_date_tuple[0]) + '-' + str(sample_date_tuple[1]) + '-' + str(sample_date_tuple[2]))
                else:
                    para_list.append(sample_date)

                patient_name = row_data[patient_name_index]
                para_list.append(patient_name)
                accpeted_date = row_data[accpeted_date_index]

                # if accpeted_date == 'Nf':
                #     para_list.append(accpeted_date)
                # elif accpeted_date == '/':
                #     para_list.append(accpeted_date)
                if type(accpeted_date) == float:
                    accpeted_date_tuple = xlrd.xldate_as_tuple(accpeted_date, 0)
                    para_list.append(
                        str(accpeted_date_tuple[0]) + '-' + str(accpeted_date_tuple[1]) + '-' + str(
                            accpeted_date_tuple[2]))
                else:
                    para_list.append(accpeted_date)


                number = row_data[number_index]
                para_list.append(number)
                register_number = row_data[register_number_index]
                para_list.append(register_number)
                gestation = row_data[gestation_index]
                para_list.append(gestation)
                pregnancy = row_data[pregnancy_index]
                para_list.append(pregnancy)
                IVFET = row_data[IVFET_index]
                para_list.append(IVFET)
                hospital = row_data[hospital_index]
                para_list.append(hospital)
                doctor = row_data[doctor_index]
                para_list.append(doctor)
                tel = row_data[tel_index]
                para_list.append(str(tel))
                status = row_data[status_index]
                para_list.append(status)

                final_period = row_data[final_period_index]
                if type(final_period) == float:
                    final_period_tuple = xlrd.xldate_as_tuple(final_period, 0)
                    para_list.append(str(final_period_tuple[0]) + '-' + str(final_period_tuple[1]) + '-' + str(
                                final_period_tuple[2]))
                else:
                    para_list.append(final_period)

                gestation_week = row_data[gestation_week_index]
                para_list.append(gestation_week)

                age = row_data[age_index]
                para_list.append(str(age))
                sample_type = row_data[type_index]
                para_list.append(sample_type)

                collection = self.database['sg_customer']
                if para_list[11] == 'Nf':
                    tel = para_list[11]
                elif para_list[11] == '/':
                    tel = para_list[11]
                else:
                    tel = int(float(para_list[11]))

                if collection.find_one({"report_num": para_list[0]}):
                    continue
                else:
                    insert_data = {
                        "report_num": para_list[0],
                        "sample_date": para_list[1],
                        "patient_name": patient_name,
                        "accpeted_date": para_list[3],
                        "number": para_list[4],
                        "register_number": para_list[5],
                        "gestation": para_list[6],
                        "pregnancy": para_list[7],
                        "IVFET": para_list[8],
                        "hospital": para_list[9],
                        "doctor": para_list[10],
                        "tel": tel,
                        "status": para_list[12],
                        "final_period": para_list[13],
                        "gestation_week": para_list[14],
                        "age": para_list[15],
                        "sample_type": para_list[16],
                    }
                    insert.append(insert_data)
        if insert == []:
            self.bind_object.logger.info('可能无新的客户信息')
        else:
            try:
                collection = self.database['sg_customer']
                collection.insert_many(insert)
            except Exception as e:
                raise Exception('插入客户信息表出错：{}'.format(e))
            else:
                self.bind_object.logger.info("插入客户信息表成功")

    def export_bed_file(self, sample, dir):
        """
        用于导出bed文件，用于后面计算
        """
        collection = self.database_ref['sg_bed']
        sample_bed = str(sample) + ".bed"
        file = os.path.join(dir, sample_bed)
        if os.path.exists(file):
            self.bind_object.logger.info("work_dir中已经存在bed文件！")
            pass
        else:
            search_result = collection.find({"sample_id": str(sample)})
            if search_result.count() != 0:
                self.bind_object.logger.info("mongo表中存在样本的bed文件！")
                final_result = search_result
                file = os.path.join(dir, sample + '.bed')
            else:
                raise Exception("没有在数据库中搜到%s"%(sample))
            with open(file, "w+") as f:
                for n in final_result:
                    f.write(str(n['chr']) + '\t' + str(n['start']) + '\t' + str(n['end']) +
                            '\t' + str(n['gc']) + '\t' + str(n['map']) + '\t' + str(n['pn']) + '\t' +
                            str(n['reads']) + '\t' + str(n['sample_id']) + '\n')
            if os.path.getsize(file):
                return file
            else:
                raise Exception("样本 %s 的bed文件为空！"%(sample))

    def add_main(self,member_id,sample_id,batch_id):
        collection = self.database['sg_main']
        insert_data={
            'member_id':member_id,
            'sample_id':sample_id,
            'batch_id':ObjectId(batch_id),
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        try:
            main_id = collection.insert_one(insert_data).inserted_id
        except Exception as e:
            raise Exception('插入主表出错：{}'.format(e))
        else:
            self.bind_object.logger.info("插入主表成功")
        return main_id

    def add_interaction(self,main_id,bw,bs,ref_group,sample_id):
        collection = self.database['sg_interaction']
        params = dict()
        params['bw'] = bw
        params['bs'] = bs
        params['ref_group'] = ref_group
        new_params = param_pack(params)
        name = 'bw-' + str(bw) + '_bs-' + str(bs)+'_ref-'+str(ref_group)
        insert_data ={
            'created_ts': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'main_id':main_id,
            'params':new_params,
            'name': name,
            "sample_id":sample_id,
            'status':'start'
        }

        try:
            interaction_id = collection.insert_one(insert_data).inserted_id
        except Exception as e:
            raise Exception('插入交互表出错：{}'.format(e))
        else:
            self.bind_object.logger.info("插入交互表成功")
        return interaction_id

    def update_interaction(self,main_id):
        collection = self.database['sg_interaction']
        try:
            collection.update({"main_id": main_id},{'$set':{'status':'start'}})
        except Exception as e:
            raise Exception('更新交互表出错：{}'.format(e))
        else:
            self.bind_object.logger.info("更新交互表成功")


    def add_fastqc(self,file):
        collection = self.database['sg_sample_fastqc']
        insert_data = {}

        file_name = file.split('/')[-1]
        sample_id = file_name.split('.')[0]

        insert_data['sample_id'] = sample_id
        if re.search(r'.*.map.valid_fastqc.html$', file):
            insert_data['fastqc_link'] = file
        else:
            insert_data['fastqc_link'] = file
        try:
            collection.insert_one(insert_data)
        except Exception as e:
            raise Exception('插入fastqc表出错：{}'.format(e))
        else:
            self.bind_object.logger.info("插入fastqc表成功")

    def add_qc(self,file):
        collection = self.database['sg_sample_qc']
        insert = {}
        file_name =file.split('/')[-1]
        sample_name = file_name.split('.')[0]
        insert["sample_id"] = sample_name
        with open(file,'rb') as f:
            for line in f:
                line = line.strip()
                line = line.split(': ')
                if re.search('.*num$', line[0]):
                    insert["num"] = line[1]
                elif re.search('.*n_map$', line[0]):
                    insert["n_map"] = line[1]
                elif re.search('.*n_dedup$', line[0]):
                    insert["n_dedup"] = line[1]
                elif re.search('.*valid_reads$', line[0]):
                    insert["valid_reads"] = line[1]
                elif re.search('.*properly_paired$', line[0]):
                    insert["properly_paired"] = line[1]
            
        try:
            collection.insert_one(insert)
        except Exception as e:
            raise Exception('插入qc表出错：{}'.format(e))
        else:
            self.bind_object.logger.info("插入qc表成功")

    def report_result(self,interaction_id,file,):
        collection = self.database['sg_interaction_result']
        collection_interaction = self.database['sg_interaction']
        collection_main = self.database['sg_main']
        insert = {}
        insert['interaction_id'] = interaction_id
        temp = collection_interaction.find_one({"_id": interaction_id})
        main_id = temp[u'"main_id']

        with open(file,'r') as f:
            for line in f:
                line = line.strip()
                line = line.split('\t')
                if line[1] == 'total_zz':
                    insert['sample_id'] = line[0]
                    if -3 <= float(line[2]) <= 3:
                        insert['result'] = 'normal'
                    else:
                        insert['result'] = 'abnormal'
                elif line[1] == '13':
                    insert['chr_13'] = line[2]
                elif line[1] == '18':
                    insert['chr_18'] = line[2]
                elif line[1] == '21':
                    insert['chr_21'] = line[2]
        try:
            collection.insert_one(insert)
            collection_main.update({'_id':main_id}, {'$set':{'result':insert['result']}})
        except Exception as e:
            raise Exception('插入结果表和更新主表出错：{}'.format(e))
        else:
            self.bind_object.logger.info("插入结果表和更新主表成功")







