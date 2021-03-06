# -*- coding: utf-8 -*-
# __author__ = 'moli.zhou'
import re
from biocluster.api.database.base import Base, report_check
import os
from biocluster.config import Config
from bson import regex
from bson import ObjectId

class TabFile(Base):
    '''
    【亲子鉴定】将生成的tab文件导入mongo之ref的数据库中，主要涉及参考库的操作
    '''
    def __init__(self, bind_object):
        super(TabFile, self).__init__(bind_object)
        # self._db_name = Config().MONGODB
        # self.mongo_client = MongoClient(Config().MONGO_BIO_URI)
        # self.database = self.mongo_client['sanger_paternity_test_v2']
        self.mongo_client = Config().biodb_mongo_client
        self.database = self.mongo_client['sanger_paternity_test_ref']


    # @report_check
    def add_pt_tab(self,sample,batch_id):
        '''
        将一批次的样本名、批次表导入库中
        母本和胎儿的analysised字段标记为None，父本标记为no，在做完分析后父本状态改为yes。
        后续按照父本为no，母本和胎儿都有tab的条件来筛选有效家系
        遇到母本胎儿重送样的情况，重新更新父本为no，以便能抓到重送样后的家系分析。
        :param sample:样本名称
        :param batch_id:批次表的_id
        '''
        if "-F" in sample:
            analysised = "no"
        else:
            analysised = "None"
        with open(sample, 'r') as f:
            for line in f:
                line = line.strip()
                line = line.split('\t')
                if line[0] != '':
                    sample_name = line[0]
                    insert_data = {
                        "analysised": analysised,
                        "batch_id": ObjectId(batch_id)
                    }
                break
            m = re.match('WQ([0-9].*)-(M|S)(.+)\.tab',sample)
            if m:
                sample_dad = 'WQ' + m.group(1) +'-F.*'
                collection = self.database['sg_pt_ref_main']
                try:
                    collection.find_one_and_update({"sample_id": {"$regex": sample_dad}},{'$set': {'analysised': 'no'}})
                except Exception as e:
                    self.bind_object.logger.error('更新重送样父本出错：{}'.format(e))
                else:
                    self.bind_object.logger.info("更新重送样父本成功")

            try:
                collection = self.database['sg_pt_ref_main']
                collection.find_one_and_update({"sample_id": sample_name},{'$set':insert_data})
            except Exception as e:
                self.bind_object.logger.error('导入tab主表出错：{}'.format(e))
            else:
                self.bind_object.logger.info("导入tab主表成功")

    def update_pt_tab(self,sample):
        '''
        分析完后，更新tab主表中的analysised字段为yes
        :param sample:样本名
        :return:
        '''
        try:
            collection = self.database['sg_pt_ref_main']
            collection.update({"sample_id": sample}, {'$set':{"analysised": "yes"}})
        except Exception as e:
            self.bind_object.logger.error('更新tab主表出错：{}'.format(e))
        else:
            self.bind_object.logger.info("更新tab主表成功")


    # @report_check
    def add_sg_pt_tab_detail(self,file_path):
        '''
        导入样本的tab文件
        :param file_path:tab文件
        :return:
        '''
        sg_pt_tab_detail = list()

        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                line = line.split('\t')
                if str(line[1]) == "chr3" and str(line[2]) == '18370866':  # xuanhongdong 20170706
                    pass
                elif str(line[1]) == 'chr17' and str(line[2]) == '7676154':
                    pass
                elif str(line[1]) == 'chr22' and str(line[2]) == '42688607':
                    pass
                elif str(line[1]) == 'chr20' and str(line[2]) == '50314010':
                    pass
                elif str(line[1]) == 'chr21' and str(line[2]) == '39445145':
                    pass
                elif str(line[1]) == 'chr12' and str(line[2]) == '8945306':
                    pass
                elif str(line[1]) == 'chr1' and str(line[2]) == '21616107':
                    pass
                elif str(line[1]) == 'chr8' and str(line[2]) == '6867054':
                    pass
                elif str(line[1]) == 'chr19' and str(line[2]) == '58387815':
                    pass
                else:
                    insert_data = {
                        "sample_id": line[0],
                        "chrom":line[1],
                        "pos": line[2],
                        "ref": line[3],
                        "alt": line[4],
                        "dp": line[5],
                        "ref_dp": line[6],
                        "alt_dp": line[7],
                    }
                    sg_pt_tab_detail.append(insert_data)
            try:
                collection = self.database['sg_pt_ref']
                collection.insert_many(sg_pt_tab_detail)
            except Exception as e:
                self.bind_object.logger.error('导入tab表格出错：{}'.format(e))
            else:
                self.bind_object.logger.info("导入tab表格成功")

    # @report_check
    def tab_exist(self, sample):
        '''
        检测样本是否已经做过fastq转tab的流程
        :param sample:
        :return:
        '''
        self.bind_object.logger.info('开始检测tab表格')
        collection = self.database['sg_pt_ref']
        result = collection.find_one({'sample_id': sample})
        if not result:
            self.bind_object.logger.info("样本{}不在数据库中，开始进行转tab文件并入库流程".format(sample))
        else:
            self.bind_object.logger.info("样本{}已存在数据库中".format(sample))
        return result

    def qc_exist(self, sample):
        '''
        检测qc表中是否有该样本的qc文件
        :param sample:
        :return:
        '''
        self.bind_object.logger.info('开始检测qc表格')
        collection = self.database['sg_pt_qc']
        result = collection.find_one({'sample_id': sample})
        if not result:
            self.bind_object.logger.info("样本{}不在数据库中，开始进行转tab文件并入库流程".format(sample))
        else:
            self.bind_object.logger.info("样本{}已存在数据库中".format(sample))
        return result


    def export_tab_file(self, sample, dir):
        '''
        导出tab file，在查重步骤时可以用到
        :param sample:
        :param dir:
        :return:
        '''
        collection = self.database['sg_pt_ref']
        sample_tab = sample + '.tab'
        file = os.path.join(dir, sample_tab)

        if os.path.exists(file):
            pass
        else:
            search_result = collection.find({"sample_id": sample})  # 读出来是个地址
            # temp = collection.find_one({"sample_id":sample})

            if search_result.count() != 0:
                final_result = search_result
                file = os.path.join(dir, sample + '.tab')
            else:
                raise Exception('意外报错：没有在数据库中搜到相应sample')
            with open(file, 'w+') as f:
                for i in final_result:
                        f.write(i['sample_id'] + '\t' + i['chrom'] + '\t' + i['pos'] + '\t'
                                + i['ref'] + '\t' + i['alt'] + '\t' + i['dp'] + '\t'
                                + i['ref_dp'] + '\t' + i['alt_dp'] + '\n')
            if os.path.getsize(file):
            # pass
                return file
            else:
                raise Exception('报错：样本数据{}的tab文件为空，可能还未下机'.format(sample))


    def dedup_sample(self):
        '''
        获取查重的父本样本list
        :return:
        '''
        collection = self.database['sg_pt_ref_main']
        # param = "WQ{}-F".format(num) + '.*'
        sample = []

        for u in collection.find({"sample_id": {"$regex": '.*-F.*'},"batch_id": {"$exists":True}}):
            sample.append(u['sample_id'])
        sample_new = list(set(sample))
        return sample_new

    def dedup_sample_report(self,num):
        '''
        在交互页面用的查重，可以选择某样本前后多少的父本进行查重
        :param num:
        :return:
        '''
        collection = self.database['sg_pt_ref_main']
        param = "WQ{}-F".format(num) + '.*'
        sample = []

        for u in collection.find({"sample_id": {"$regex": param},"batch_id": {"$exists":True}}):
            sample.append(u['sample_id'])
        sample_new = list(set(sample))
        return sample_new

    def sample_qc(self, file, sample_id):
        '''
        按照质控标准，一方面导入qc文件，一方面做判断来看该样本是否满足质控条件
        :param file:
        :param sample_id:
        :return:
        '''
        qc_detail = list()
        with open(file,'r') as f:
            for line in f:
                line = line.strip()
                line = line.split(":")

                if line[0] == 'dp':
                    if float(line[1]) >=40:
                        color = "green"
                    elif float(line[1]) < 30:
                        color = "red"
                    else:
                        color = "yellow"
                elif line[0] == "num__chrY":
                    if "-F" in sample_id:
                        if float(line[1]) < 10:
                            color = 'red'
                        else:
                            color = 'green'
                    elif "-M" in sample_id:
                        if float(line[1]) >= 3:
                            color = 'red'
                        elif float(line[1]) == 0:
                            color ='green'
                        else:
                            color = "yellow"
                    elif "-S" in sample_id:
                        line[1]='/'
                        color = ''
                elif line[0] == 'num':
                    num_M = int(line[1])/1000000
                    if "-M" in sample_id or "-F" in sample_id:
                        if num_M <3:

                            color = "red"
                        elif 3 <= num_M <=4:
                            color = 'yellow'
                        else:
                            color = 'green'
                    else:
                        if num_M <=7.5:
                            color = 'red'
                        elif num_M > 8.5:
                            color = 'green'
                        else:
                            color = 'yellow'
                else:
                    color = ''
                    if line[0] == "n_dedup" or line[0] == "n_mapped" or line[0]\
                            == "n_mapped_dedup" :
                        line[1] = format(int(line[1]), ',')
                    elif line[0] == "0Xcoveragerate" or line[0] == "15Xcoveragerate" or line[0] == "50Xcoveragerate":
                        line[1] = round(float(line[1]),4)

                insert_data = {
                    "qc": line[0],
                    "value": line[1],
                    "sample_id":sample_id,
                    "color":color
                }
                qc_detail.append(insert_data)
            try:
                collection = self.database['sg_pt_qc']
                collection.insert_many(qc_detail)
            except Exception as e:
                self.bind_object.logger.error('导入qc表格出错：{}'.format(e))
            else:
                self.bind_object.logger.info("导入qc表格成功")

    def sample_qc_addition(self,sample_id):
        '''
        格外的qc文件，主要是杂捕中会考察
        :param sample_id:
        :return:
        '''
        collection = self.database['sg_pt_qc']
        insert = []

        find_n_hit = collection.find_one({"sample_id":sample_id, 'qc':'n_hit'})
        n_hit = float(find_n_hit['value'])
        n_hit_new = format(n_hit,',')

        find_num = collection.find_one({"sample_id":sample_id, "qc":'num'})
        num = float(find_num['value'])
        num_new = format(num,',')

        ot = round(n_hit/num, 4)
        if ot <= 0.015:
            color_ot = 'red'
        elif 0.015 < ot <= 0.025:
            color_ot = 'yellow'
        elif ot > 0.025:
            color_ot = 'green'

        find_n_hit_dedup = collection.find_one({"sample_id": sample_id, 'qc': 'n_hit_dedup'})
        n_hit_dedup = float(find_n_hit_dedup['value'])
        ot_dedup = round(n_hit_dedup/n_hit,4)
        n_hit_dedup_new = format(n_hit_dedup,',')

        if ot_dedup<=0.2:
            color_ot_dedup = 'red'
        elif 0.2<ot_dedup<=0.3:
            color_ot_dedup = 'yellow'
        elif ot_dedup >0.3:
            color_ot_dedup = 'green'

        insert_data1 = {
            "qc":"ot",
            "value":ot,
            "sample_id":sample_id,
            "color": color_ot
        }
        insert.append(insert_data1)

        insert_data2 = {
            "qc": "ot_dedup",
            "value": ot_dedup,
            "sample_id": sample_id,
            "color": color_ot_dedup
        }
        insert.append(insert_data2)
        try:
            collection.insert_many(insert)
            collection.find_one_and_update({"sample_id":sample_id,'qc':'n_hit_dedup'},{"$set":{"value":n_hit_dedup_new}})
            collection.find_one_and_update({"sample_id":sample_id,'qc':'n_hit'},{"$set":{"value":n_hit_new}})
            collection.find_one_and_update({"sample_id":sample_id,'qc':'num'},{"$set":{"value":num_new}})
        except Exception as e:
            self.bind_object.logger.error('计算并导入ot出错：{}'.format(e))
        else:
            self.bind_object.logger.info("计算并导入ot成功")


    def family_unanalysised(self):
        '''
        挑选出符合条件的家系（胎儿和母本存在，父本标记还未分析过）
        :return:
        '''
        family_id = []
        collection = self.database['sg_pt_ref_main']
        sample = collection.find({"analysised":'no'})
        for i in sample:
            dad_id = []
            mom_id =[]
            preg_id =[]
            m = re.search(r'WQ([0-9]*)-F.*', i['sample_id'])
            family = m.group(1)
            dad_id.append(i['sample_id'])
            dad_id = list(set(dad_id))
            mom = "WQ" + family + "-M.*"
            sample_mom = collection.find({"sample_id": {"$regex": mom},"analysised":"None"})
            for s in sample_mom:
                mom_id.append(s['sample_id'])
                mom_id = list(set(mom_id))
            preg = "WQ" +family + "-S.*"
            sample_preg = collection.find({"sample_id": {"$regex": preg},"analysised":"None"})
            for n in sample_preg:
                preg_id.append(n['sample_id'])
                preg_id = list(set(preg_id))

            if sample_mom and sample_preg:
                for dad in dad_id:
                    for mom in mom_id:
                        for preg in preg_id:
                            family_member = []
                            family_member.append(dad)
                            family_member.append(mom)
                            family_member.append(preg)

                            family_id.append(family_member)
                # if dad_id not in family_id and mom_id not in family_id and preg_id not in family_id:
                # 	family_id.append(dad_id)
                # 	family_id.append(mom_id)
                # 	family_id.append(preg_id)
                # 	final.append(family_id)
            else:
                self.bind_object.logger.info("家系数据还未全部下机")
                continue
        return family_id

    def type(self,sample_id):
        '''
        确认实验类型，是pt or dcpt
        :param sample_id:
        :return:
        '''
        collection = self.database['sg_pt_ref_main']
        sample = collection.find_one({"sample_id": sample_id})
        if not sample:
            raise Exception('{}样本在库中找不到类型'.format(sample_id))
        return sample['type']

    def sample_qc_dc(self, file, sample_id):
        '''
        多重实验的qc文件入库（不同于杂捕的质控要求）
        :param file:
        :param sample_id:
        :return:
        '''
        qc_detail = list()
        with open(file,'r') as f:
            for line in f:
                line = line.strip()
                line = line.split(":")

                if line[0] == 'dp1':
                    if float(line[1]) >=50:
                        color = "green"
                    elif float(line[1]) < 30:
                        color = "red"
                    else:
                        color = "yellow"
                elif line[0] == "num__chrY":
                    if "-F" in sample_id:
                        if float(line[1]) <= 2:
                            color = 'red'
                        else:
                            color = 'green'
                elif line[0] == '0Xcoveragerate1':
                    if float(line[1]) <0.9:
                        color = 'red'
                    else:
                        color = 'green'
                elif line[0] == '15Xcoveragerate1':
                    if float(line[1]) < 0.8:
                        color = 'red'
                    else:
                        color = 'green'
                elif line[0] == '50Xcoveragerate1':
                    if float(line[1]) < 0.65:
                        color = 'red'
                    else:
                        color = 'green'
                elif line[0] == 'num':
                    num_M = int(line[1])/1000000
                    if num_M <= 1:
                        color = "red"
                    else:
                        color = 'green'
                else:
                    color = ''
                    if line[0] == "n_dedup" or line[0] == "n_mapped":
                        line[1] = format(int(line[1]), ',')
                    elif line[0] == "0Xcoveragerate1" or line[0] == "15Xcoveragerate1" or \
                                    line[0] == "50Xcoveragerate" or line[0] == "50Xcoveragerate1" or line[0] =="100Xcoveragerate1":
                        line[1] = round(float(line[1]),4)

                insert_data = {
                    "qc": line[0],
                    "value": line[1],
                    "sample_id":sample_id,
                    "color":color
                }
                qc_detail.append(insert_data)
            try:
                collection = self.database['sg_pt_qc']
                collection.insert_many(qc_detail)
            except Exception as e:
                self.bind_object.logger.error('导入qc表格出错：{}'.format(e))
            else:
                self.bind_object.logger.info("导入qc表格成功")

    def sample_qc_addition_dc(self,sample_id):
        collection = self.database['sg_pt_qc']
        insert = []

        find_n_hit = collection.find_one({"sample_id":sample_id, 'qc':'n_hit'})
        n_hit = float(find_n_hit['value'])
        n_hit_new = format(n_hit,',')

        find_num = collection.find_one({"sample_id":sample_id, "qc":'num'})
        num = float(find_num['value'])
        num_new = format(num,',')

        ot = round(n_hit/num, 4)
        if ot <= 0.025:
            color_ot = 'red'
        elif 0.025 < ot <= 0.08:
            color_ot = 'yellow'
        elif ot > 0.08:
            color_ot = 'green'

        insert_data1 = {
            "qc":"ot",
            "value":ot,
            "sample_id":sample_id,
            "color": color_ot
        }
        insert.append(insert_data1)

        try:
            collection.insert_many(insert)
            collection.find_one_and_update({"sample_id":sample_id,'qc':'n_hit'},{"$set":{"value":n_hit_new}})
            collection.find_one_and_update({"sample_id":sample_id,'qc':'num'},{"$set":{"value":num_new}})
        except Exception as e:
            self.bind_object.logger.error('计算并导入ot出错：{}'.format(e))
        else:
            self.bind_object.logger.info("计算并导入ot成功")

    def judge_qc(self, sample_id):  # 20170707 zhouxuan
        collection = self.database['sg_pt_qc']
        if collection.find_one({"sample_id": sample_id, 'color': 'red'}):
            return 'red'
        else:
            return 'green'
