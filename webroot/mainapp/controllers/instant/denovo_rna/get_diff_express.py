# -*- coding: utf-8 -*-
# __author__ = 'qiuping'
import web
import json
import threading
import datetime
from bson.son import SON
from mainapp.libs.signature import check_sig
from mainapp.config.db import get_mongo_client
from biocluster.config import Config
from bson.objectid import ObjectId


class GetDiffExpress(object):
    def __init__(self):
        self.client = get_mongo_client()
        self.db = self.client[Config().MONGODB + '_rna']
        self.samples = []  # 包含的样本列表
        self.diff_genes = []  # 筛选出来的差异基因
        self.params = {}

    @check_sig
    def POST(self):
        data = web.input()
        print data
        return_result = self.check_options(data)
        if return_result:
            info = {"success": False, "info": '+'.join(return_result)}
            return json.dumps(info)
        ids = data.express_diff_id
        compare_list = data.compare_list
        # compare_list.sort()
        self.params['express_diff_id'] = ids
        self.params['is_sum'] = data.is_sum
        self.params['compare_list'] = compare_list
        self.params = json.dumps(self.params, sort_keys=True, separators=(',', ':'))
        self.get_diff_exp(data.express_diff_id, compare_list, data.is_sum)
        info = dict()
        info["success"] = True
        info["info"] = "已成功完成计算"
        return json.dumps(info)

    def check_options(self, data):
        """
        检查网页端传进来的参数是否正确
        """
        params_name = ['express_diff_id', 'is_sum', 'compare_list', 'submit_location']
        success = []
        for names in params_name:
            if not (hasattr(data, names)):
                success.append("缺少参数!")
        if not data.compare_list:
            success.append("传入的conpare_list：{}为空".format(data.compare_list))
        return success

    def get_diff_gene(self, table_id, compare_group):
        """
        获取单个两两比较的差异统计分析表中的差异基因
        table_id: sg_denovo_express_diff的id
        compare_group: 两两比较的样本或分组名字，元组，比如（A，B），A对应mongo表中的name，B对应mongo表中的compare_name
        return:: diff_gene: 差异基因列表
        """
        collection = self.db['sg_denovo_express_diff_detail']
        result = collection.find({'$and': [{'express_diff_id': ObjectId(table_id), 'name': compare_group[0], 'compare_name': compare_group[1]}]})
        diff_gene = []
        if not result.count():
            raise Exception('没有找到差异表达统计id对应的表，请检查传入的id是否正确')
        for row in result:
            samples = row['specimen']
            if float(row['frd']) <= 0.05:
                diff_gene.append(row['gene_id'])
        return (diff_gene, samples)

    def get_diff_exp(self, table_id, compare_list, is_sum=True):
        """
        筛选差异基因矩阵，导入mongodb中
        table_id: sg_denovo_express_diff的id
        compare_list: 网页端传入的两两比较的组合，列表
        is_sum: 合成差异基因矩阵的方式
        """
        threads = []
        for g in list(compare_list):
            t = mythreading(self.get_diff_gene, argu=(table_id, g))
            threads.append(t)
        for th in threads:
            th.setDaemon = True
            th.start()
        th.join()
        diff_list = []
        if is_sum:
            for i in threads:
                diff_list.append(i.diff_gene)
                self.samples.append(i.sample)
            diff_list = list(set(diff_list))
        else:
            diff_list = threads[0].diff_gene
            for i in threads[1:]:
                diff_list = list(set(diff_list) & set(i.diff_list))
                self.samples.append(i.sample)
        self.diff_genes = diff_list
        self.samples = list(set(self.samples))
        if not self.diff_genes:
            info = {"success": False, "info": "所选的筛表条件没有找到差异基因，请重设条件！"}
            return json.dumps(info)
        insert_id, from_id = self.create_express(table_id)
        self.create_express_detail(from_id, insert_id)

    def create_express(self, table_id, name=None):
        """
        更新sg_denovo_express
        """
        collection = self.db['sg_denovo_express_diff']
        table = collection.find_one({'_id': ObjectId(table_id)})
        project = table['project_sn']
        task = table['task_id']
        desc = "差异基因筛选后的表达量表"
        insert_data = {
            "project_sn": project,
            "task_id": task,
            "status": "end",
            "desc": desc,
            "specimen": self.samples,
            "created_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "name": name if name else "diff_gene_express_matrix",
            "params": self.params
        }
        insert_coll = self.db['sg_denovo_express']
        insert_id = insert_coll.insert_one(insert_data).inserted_id
        from_express_id = table['express_id']
        return insert_id, from_express_id

    def create_express_detail(self, from_express_id, new_express_id):
        """往sg_denovo_express_detail导入差异基因的相应的数据信息"""
        find_coll = self.db['sg_denovo_express_detail']
        insert_data = []
        find_result = find_coll.find({'express_id': ObjectId(from_express_id)})
        sam_fpkm = []
        for sam in self.samples:
            sam_fpkm.append(sam + '_gene_fpkm')
        for row in find_result:
            if row['gene_id'] in self.diff_genes:
                data = [
                    ('gene_id', row['gene_id']),
                    ('express_id', new_express_id)
                ]
                for fpkm in sam_fpkm:
                    data.append((fpkm, row[fpkm]))
                data_son = SON(data)
                insert_data.append(data_son)
        try:
            insert_coll = self.db['sg_denovo_express_detail']
            insert_coll.insert_many(insert_data)
        except Exception, e:
            raise Exception('导入差异基因矩阵表出错{}'.format(e))


class mythreading(threading.Thread):
    def __init__(self, func, *argu, **kwargu):
        super(mythreading, self).__init__()
        self.func = func
        self.arge = argu
        self.kwargu = kwargu
        self.diff_gene = []
        self.sample = []

    def run(self):
        (self.diff_gene, self.samples) = self.func(*self.argu, **self.kwargu)
