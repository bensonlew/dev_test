# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'
import re
from biocluster.api.database.base import Base, report_check
from biocluster.config import Config
from bson import ObjectId
import xlrd
import datetime
import os


class PtCustomer(Base):
    '''
    将生成的tab文件导入mongo之ref的数据库中
    '''
    def __init__(self, bind_object):
        super(PtCustomer, self).__init__(bind_object)
        self.mongo_client = Config().mongo_client
        self.database = self.mongo_client[Config().MONGODB+'_paternity_test']

        self.mongo_client_ref = Config().biodb_mongo_client
        self.database_ref = self.mongo_client_ref['sanger_paternity_test_ref']

    def add_pt_customer(self, main_id=None, customer_file=None):
        if customer_file == "None":
            self.bind_object.logger.info("缺少家系表")
        if main_id == "None":
            self.bind_object.logger.info("缺少主表id")
        try:
            bk = xlrd.open_workbook(customer_file)
            sh = bk.sheet_by_name(u'Report')
        except:
            self.bind_object.logger.info('pt家系表-表单名称不对')
            raise Exception('pt家系表-表单名称不对')
        nrows = sh.nrows
        insert = []  # 获取各行数据
        for i in range(0, nrows):
            row_data = sh.row_values(i)
            if i == 0:
                try:
                    contrast_num_index = row_data.index(u'\u8ba2\u5355\u5185\u90e8\u7f16\u53f7')  # 订单内部编号
                    ask_person_index = row_data.index(u'\u7533\u8bf7\u4eba')  # 申请人
                    mother_name_index = row_data.index(u'\u6bcd\u672c\u540d\u79f0')  # 母本名称
                    mother_type_index = row_data.index(u'\u6bcd\u672c\u7c7b\u578b')  # 母本类型
                    mom_id_index = row_data.index(u'\u6bcd\u672c\u7f16\u53f7')  # 母本编号
                    father_name_index = row_data.index(u'\u7236\u672c\u540d\u79f0')  # 父本名称
                    father_type_index = row_data.index(u'\u7236\u672c\u7c7b\u578b')  # 父本类型
                    dad_id_index = row_data.index(u'\u7236\u672c\u7f16\u53f7')  # 父本编号
                    ask_time_index = row_data.index(u'\u7533\u8bf7\u65e5\u671f')     # 申请日期
                    f_accept_time_index = row_data.index(u'\u0046\u53d7\u7406\u65e5\u671f')  # F受理日期
                    m_accept_time_index = row_data.index(u'\u004d\u53d7\u7406\u65e5\u671f')  # M受理日期
                    f_result_time_index = row_data.index(u'\u0046\u9274\u5b9a\u65e5\u671f')  # F鉴定日期
                    m_result_time_index = row_data.index(u'\u004d\u9274\u5b9a\u65e5\u671f')  # M鉴定日期
                    family_mom_id = row_data.index(u'\u4eb2\u672c\u0028\u6bcd\u672c\u0029')  # 亲本(母本)
                    family_dad_id = row_data.index(u'\u4eb2\u672c\u0028\u7236\u672c\u0029')  # 亲本(父本)
                    report_status = row_data.index(u'\u52a0\u6025')  # 加急 (标定出了报告立即置顶)
                    # son_type_index = row_data.index(u'\u8865\u9001\u6837\u672c\u80ce\u513f\u4fe1\u606f')  # 补送样本胎儿信息
                except:
                    self.bind_object.logger.info("pt家系表的表头信息不全")
                    raise Exception('pt家系表的表头信息不全')
            else:
                if row_data[contrast_num_index] == "":
                    break
                if row_data[mother_type_index] == '' or row_data[father_type_index] == '':
                    continue
                if row_data[dad_id_index] != '' and row_data[mom_id_index] != '':
                    family_name = row_data[contrast_num_index] + "-" + row_data[family_dad_id] + "-" + row_data[family_mom_id]
                    collection = self.database["sg_pt_customer"]
                    result = collection.find_one({"name": family_name})
                    if result:  # 不会有两条信息的name一致
                        continue
                    else:
                        if row_data[father_type_index] == '亲子父本全血':
                            father_type = '全血'
                        else:
                            father_type = row_data[father_type_index]
                        mom_id = row_data[contrast_num_index] + '-' + row_data[family_mom_id].split('-')[0]  # 母本编号
                        dad_id = row_data[contrast_num_index] + '-' + row_data[family_dad_id].split('-')[0]  # 父本编号
                        if row_data[f_accept_time_index] > row_data[m_accept_time_index]:
                            accept_time = row_data[f_accept_time_index],
                            result_time = row_data[f_result_time_index],
                        else:
                            accept_time = row_data[m_accept_time_index],
                            result_time = row_data[m_result_time_index],
                        insert_data = {
                            "pt_datasplit_id": ObjectId(main_id),  # 拆分批次
                            "pt_serial_number": row_data[contrast_num_index],  # 所谓的检案号
                            "ask_person": row_data[ask_person_index],  # 申请人
                            "mother_name": row_data[mother_name_index],  #
                            "mother_type": row_data[mother_type_index],
                            "mom_id_": row_data[family_mom_id],
                            "mom_id": mom_id,
                            "father_name": row_data[father_name_index],
                            "father_type": father_type,  # father_type 不能写 亲子父本全血
                            "father_type_origin": row_data[father_type_index],  # 保存原始数据
                            "dad_id_": row_data[family_dad_id],
                            "dad_id": dad_id,
                            "ask_time": row_data[ask_time_index],
                            "F_accept_time": row_data[f_accept_time_index],
                            "F_result_time": row_data[f_result_time_index],
                            "M_accept_time": row_data[m_accept_time_index],
                            "M_result_time": row_data[m_result_time_index],
                            "accept_time": accept_time[0],
                            "result_time": result_time[0],
                            "name": family_name,
                            "report_status": row_data[report_status],
                            'update_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        insert.append(insert_data)
                else:
                    continue
        if len(insert) == 0:
            self.bind_object.logger.info("没有新的家系需要导入数据库！")
        else:
            try:
                collection = self.database['sg_pt_customer']
                collection.insert_many(insert)
            except Exception as e:
                self.bind_object.logger.error('导入家系表表出错：{}'.format(e))
                raise Exception('导入家系表表出错：{}'.format(e))
            self.bind_object.logger.info("导入家系表成功")

    def add_data_dir(self, dir_name, wq_dir, ws_dir, undetermined_dir):
        insert_data = {
            "data_name": dir_name,
            "wq_dir": wq_dir,
            "ws_dir": ws_dir,
            "undetermined_dir": undetermined_dir
        }
        try:
            collection = self.database["sg_med_data_dir"]
            if collection.find_one({"data_name": dir_name}):
                collection.update_one({"data_name": dir_name}, {'$set':
                                                            {"wq_dir": wq_dir,
                                                            "ws_dir": ws_dir,
                                                            "undetermined_dir": undetermined_dir}})
            else:
                collection.insert_one(insert_data)
        except Exception as e:
            self.bind_object.logger.info('导入拆分结果路径出错：{}'.format(e))
        else:
            self.bind_object.logger.info('导入拆分结果路径成功')

    def get_wq_dir(self, file_name):
        main_collection = self.database["sg_med_data_dir"]
        result = main_collection.find_one({"data_name": file_name})
        dir_list = []
        if result:
            dir_list.append(result["wq_dir"])
            dir_list.append(result["ws_dir"])
            dir_list.append(result["undetermined_dir"])
            return dir_list
        else:
            return dir_list

    def add_sample_type(self, file):
        file_name = os.path.basename(file)
        data_id = ("_").join(file_name.split('_')[0:-1])
        insert = []
        with open(file, 'r') as f:
            for line in f:
                line = line.strip()
                line = line.split('\t')
                if re.match('WQ([0-9]*)-.*', line[3]):
                    insert_data = {
                        "type": line[2].strip(),
                        "sample_id": line[3].strip(),
                        'split_data_name': data_id,
                    }
                    collection = self.database_ref['sg_pt_ref_main']
                    if collection.find_one({"sample_id": line[3]}):
                        pass
                    else:
                        insert.append(insert_data)
            if insert:
                try:
                    collection = self.database_ref['sg_pt_ref_main']
                    collection.insert_many(insert)
                except Exception as e:
                    self.bind_object.logger.error('导入ref类型出错：{}'.format(e))
                else:
                    self.bind_object.logger.info("导入ref类型成功")
            else:
                self.bind_object.logger.info("没有插入样本信息")

    def update_pt_family(self, family_id, accept_time):
        try:
            collection = self.database["sg_pt_customer"]
            if collection.find_one({"pt_serial_number": family_id}):
                collection.update_many({"pt_serial_number": family_id}, {'$set':
                                                                            {"accept_time": accept_time,
                                                                             'update_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}})
            else:
                self.bind_object.logger.info('不存在该家系，请确认是否存在胎儿信息：{}'.format(family_id))
        except Exception as e:
            self.bind_object.logger.info('导入拆分结果路径出错：{}'.format(e))
        else:
            self.bind_object.logger.info('导入拆分结果路径成功')

    def family_search(self, sample_list):
        ref_main_collection = self.database_ref['sg_pt_ref_main']   # 正式机与测试机不一样，现在不要ref
        customer_message_collection = self.database["sg_pt_customer"]
        family_id_list = []
        for i in sample_list:
            m = re.match('WQ([0-9]{8,})-(M|F|S)(.*)', i)
            family_id = 'WQ' + m.group(1)
            if family_id in family_id_list:
                continue
            else:
                family_id_list.append(family_id)
            sample_dad = ref_main_collection.find({"sample_id": {"$regex": family_id + "-F.*"}})
            sample_mom = ref_main_collection.find({"sample_id": {"$regex": family_id + "-M.*"}})
            sample_preg = ref_main_collection.find({"sample_id": {"$regex": family_id + "-S.*"}})
            if sample_dad and sample_mom and sample_preg:  # 家系信息只需要爸爸妈妈 不需要胎儿
                dad_id = []
                mom_id = []
                for d in sample_dad:
                    dad_id.append(d['sample_id'])
                    dad_id = list(set(dad_id))
                for m in sample_mom:
                    mom_id.append(m['sample_id'])
                    mom_id = list(set(mom_id))
                for dad in dad_id:
                    if 'T' in dad:
                        n = re.match('(.*)-T(.*)', dad)
                        dad_ = n.group(1)
                    else:
                        dad_ = dad
                    for mom in mom_id:
                        if 'T' in mom:
                            l = re.match('WQ([0-9]{8,})-(M.*)-T(.*)', mom)
                        else:
                            l = re.match('WQ([0-9]{8,})-(M.*)', mom)
                        mom_ = l.group(2)
                        name = dad_ + '-' + mom_
                        if customer_message_collection.find_one({"name": name}):
                            self.bind_object.logger.info('{}——该家系信息完整'.format(name))
                        else:
                            self.bind_object.logger.info('{}——该家系信息不存在，请查看{}和{}命名是否正确'.format(name, mom, dad))
                            raise Exception('{}——该家系信息不存在，请查看{}和{}命名是否正确'.format(name, mom, dad))
            else:
                continue

    '''
    def family_search(self, family_id,  mom_id_=None, dad_id_=None):
        collection = self.database["sg_pt_customer"]
        if mom_id_ == None and collection.find_one({"pt_serial_number": family_id, 'dad_id_': dad_id_}):
            return 'True'
        elif dad_id_ == None and collection.find_one({"pt_serial_number": family_id, 'mom_id_': mom_id_}):
            return 'True'
        else:
            return "False"
    '''


