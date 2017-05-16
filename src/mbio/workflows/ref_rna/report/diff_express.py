# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'

"""有参转录组表达差异分析"""

from biocluster.workflow import Workflow
from biocluster.config import Config
import os
import re
from bson.objectid import ObjectId
from mainapp.libs.param_pack import group_detail_sort

class DiffExpressWorkflow(Workflow):
    """
    报告中调用组间差异性分析检验时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(DiffExpressWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "express_file", "type": "string"},
            {"name":"count","type":"outfile",'format':"rna.express_matrix"},
            {"name":"edger_group","type":"outfile",'format':"sample.group_table"},
            {"name": "group_detail", "type": "string"},
            {"name": "fc", "type": "float"},
            {"name": "group_id", "type": "string"},
            {"name":"group_id_id","type":"string"},  #group_id_id，字符串格式，不是to_file生成的edger_group文件
            {"name":"express_method","type":"string"},
            {"name": "update_info", "type": "string"},
            {"name": "control_file", "type": "string"},
            {"name": "class_code", "type":"string"},
            {"name": "diff_express_id", "type": "string"},
            {"name": "diff_method", "type": "string","default":"edgeR"},
            {"name": "type","type": "string"},
            {"name":"log","type":"string"},
            {"name":"pvalue_padjust","type":"string","default":"padjust"},
            {"name":"pvalue","type":"float","default":0.01}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.diff_exp = self.add_tool("rna.diff_exp")
        self.output_dir = self.diff_exp.output_dir
        self.group_spname = dict()

    def run_diff_exp(self):
        exp_files = self.option("express_file").split(',')
        options = {
            "count": exp_files[1],
            "fpkm": exp_files[0],
            "control_file": self.option("control_file"),
            # "edger_group":self.option("group_id"),
            "method": self.option("diff_method"),
            "fc": self.option("fc"),
        }
        if self.option("pvalue_padjust") == "padjust":
            options["diff_fdr_ci"]=self.option("pvalue")
        if self.option("pvalue_padjust") == "pvalue":
            options["diff_ci"] = self.option("pvalue")
        self.option("count").set_path(exp_files[1])
        if self.option("group_id") != "all":  
            options['edger_group'] = self.option("group_id")
        self.diff_exp.set_options(options)
        self.diff_exp.on("end", self.set_db)
        self.diff_exp.run()

    def set_db(self):
        """
        保存结果表保存到mongo数据库中
        """
        api_diff_exp = self.api.refrna_express
        diff_files = os.listdir(self.output_dir)
        
        if self.option("group_id") == "all":
            # self.samples = self.diff_exp.option('count').prop['sample']
            self.samples = self.option("count").prop['sample']
            self.logger.info(self.samples)
            self.group_spname['all'] = self.samples
        else:
            self.group_spname = self.diff_exp.option('edger_group').get_group_spname()
            edger_group = self.option("group_id")
            self.samples=[]
            with open(edger_group,'r+') as f1:
                f1.readline()
                for lines in f1:
                    line = lines.strip().split("\t")
                    self.samples.append(line[0])
            # self.samples = self.option('group_id').prop['sample']
        compare_column = list()
        for f in diff_files:
            if re.search(r'_edgr_stat.xls$', f):
                #获得所有比较的差异分析文件
                con_exp = f.split('_edgr_stat.xls')[0].split('_vs_')
                compare_column.append('|'.join(con_exp))
                print self.output_dir + '/' + f
                name = con_exp[0]
                compare_name = con_exp[1]
                
                """添加diff_detail表"""
                api_diff_exp.add_express_diff_detail(name=name,compare_name=compare_name, express_diff_id=self.option("diff_express_id"),\
                            diff_stat_path=self.output_dir + '/' + f, workflow=False,class_code = self.option("class_code"),query_type = self.option("type"))
                """添加geneset主表"""
                # geneset_up_id, geneset_down_id = api_diff_exp.add_geneset(diff_stat_path = self.output_dir + '/' + f,name=name,compare_name=compare_name,\
                            # group_id=self.option("group_id_id"),express_method=self.option("express_method"),type=self.option("type"))
                up_id = api_diff_exp.add_geneset(diff_stat_path = self.output_dir + '/' + f,name=name,compare_name=compare_name,\
                            group_id=self.option("group_id_id"),express_method=self.option("express_method"),type=self.option("type"),up_down='up')
                down_id = api_diff_exp.add_geneset(diff_stat_path = self.output_dir + '/' + f,name=name,compare_name=compare_name,\
                            group_id=self.option("group_id_id"),express_method=self.option("express_method"),type=self.option("type"),up_down='down')
                up_down_id = api_diff_exp.add_geneset(diff_stat_path = self.output_dir + '/' + f,name=name,compare_name=compare_name,\
                            group_id=self.option("group_id_id"),express_method=self.option("express_method"),type=self.option("type"),up_down='up_down')
                """添加geneset_detail表-up,down"""
                api_diff_exp.add_geneset_detail(geneset_id = up_id,diff_stat_path = self.output_dir + '/' + f, fc=self.option("fc"),up_down='up')
                api_diff_exp.add_geneset_detail(geneset_id = down_id,diff_stat_path = self.output_dir + '/' + f, fc=self.option("fc"),up_down='down')
                api_diff_exp.add_geneset_detail(geneset_id = up_down_id,diff_stat_path = self.output_dir + '/' + f, fc=self.option("fc"),up_down='up_down')
                
                # api_diff_exp.add_geneset(geneset_id=geneset_id, diff_stat_path=self.output_dir + '/' + f)
                # express_diff_id = api_diff_exp.test_main_table(express_id=self.option('diff_express_id'))
                # api_diff_exp.express_diff_up(group=con_exp, express_diff_id=self.option('diff_express_id'), diff_stat_path=self.output_dir + '/' + f )
                # geneset_id = api_diff_exp.express_diff_up(group=con_exp, express_diff_id=self.option('diff_express_id'),group_id=self.option('group_id'),diff_stat_path=self.output_dir + '/' + f )
                # api_diff_exp.add_geneset(geneset_id=geneset_id, diff_stat_path=self.output_dir + '/' + f)
                # geneset_up_id = api_diff_exp.express_diff_up_up(group=con_exp, express_diff_id=self.option('diff_express_id'),group_id=self.option('group_id'),diff_stat_path=self.output_dir + '/' + f )
                # api_diff_exp.add_geneset_up(geneset_id=geneset_up_id, diff_stat_path=self.output_dir + '/' + f)
                # geneset_down_id = api_diff_exp.express_diff_up_down(group=con_exp, express_diff_id=self.option('diff_express_id'),group_id=self.option('group_id'),diff_stat_path=self.output_dir + '/' + f )
                # api_diff_exp.add_geneset_down(geneset_id=geneset_down_id, diff_stat_path=self.output_dir + '/' + f)
        """添加summary表"""
        if os.path.exists(self.output_dir + '/merge.xls'):
            api_diff_exp.add_diff_summary_detail(diff_express_id=self.option('diff_express_id'),count_path =self.output_dir + '/merge.xls')
        else:
            raise Exception("此次差异分析没有生成summary表！")
        """更新主表信息"""
        self.update_express_diff(table_id=self.option('diff_express_id'), compare_column=compare_column, group_detail=self.group_spname, samples=self.samples)
        self.end()

    def update_express_diff(self, table_id, compare_column, group_detail, samples):
        client = Config().mongo_client
        db_name = Config().MONGODB + '_ref_rna'
        self.logger.info(db_name)
        self.logger.info('haha')
        collection = client[db_name]['sg_express_diff']
        """方便前端取数据, 生成compare_column_specimen"""
        if self.option("group_id") != "all":
            compare_column_specimen={}
            group_detal_dict = json.loads(self.option("group_detail"))
            for group,samples in group_detal_dict.items():
                compare_column_specimen[group]=samples
            collection.update({'_id': ObjectId(table_id)}, {'$set': {'group_detail': group_detail, 'compare_column': compare_column, 'specimen': samples,'compare_column_specimen':compare_column_specimen}})
        else:
            collection.update({'_id': ObjectId(table_id)}, {'$set': {'group_detail': group_detail, 'compare_column': compare_column, 'specimen': samples}})

    def run(self):
        self.run_diff_exp()
        super(DiffExpressWorkflow, self).run()
