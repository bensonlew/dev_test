# -*- coding: utf-8 -*-
# __author__ = 'zhangpeng'

"""有参转录组表达差异分析"""

from biocluster.workflow import Workflow
from biocluster.config import Config
import os
import re
from bson.objectid import ObjectId


class DiffExpressWorkflow(Workflow):
    """
    报告中调用组间差异性分析检验时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(DiffExpressWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "express_file", "type": "string", 'default': "none"},
            {"name":"count","type":"outfile"},
            {"name":"edger_group","type":"outfile"}
            {"name": "group_detail", "type": "string"},
            {"name": "fc", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "control_file", "type": "string"},
            # {"name": "ci", "type": "float"},
            {"name": "diff_express_id", "type": "string"},
            {"name": "diff_method", "type": "string"},
            {"name": "type","type": "string"},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.diff_exp = self.add_tool("ref_rna.express.diff_exp")
        self.output_dir = self.diff_exp.output_dir
        self.group_spname = dict()

    def run_diff_exp(self):
        exp_files = self.option("express_file").split(',')
        options = {
            "count": exp_files[1],
            "fpkm": exp_files[0],
            "control_file": self.option("control_file"),
            "edger_group":self.option("group_id"),
            "diff_ci": 0.5,
            "method": self.option("diff_method"),
            "diff_fdr_ci": 2,
            "fc": self.option("fc"),
        }
        self.option("count").set_path(exp_files[1])
        
        if self.option("group_id") != "all":
            options['edger_group'] = self.option("group_file")
        self.diff_exp.set_options(options)
        self.diff_exp.on("end", self.set_db)
        self.diff_exp.run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        relpath = [
            [".", "", "结果输出目录"],
            ["diff_fpkm", "xls", "差异基因表达量表"],
            ["diff_count", "xls", "差异基因计数表"],
            ["merge.xls", "xls", "组合数据表格"],
            ["merge_name.xls", "xls", "组合汇众说明"]
        ]
        result_dir.add_regexp_rules([
            [r"_edgr_stat\.xls$", "xls", "edger统计结果文件"]
        ])
        result_dir.add_relpath_rules(relpath)
        super(DiffExpressWorkflow, self).end()

    def set_db(self):
        """
        保存结果表保存到mongo数据库中
        """
        api_diff_exp = self.api.ref_express
        diff_files = os.listdir(self.output_dir)
        
        if self.option("group_id") == "all":
            # self.samples = self.diff_exp.option('count').prop['sample']
            self.samples = self.option("count").prop['sample']
            self.logger.info(self.samples)
            self.group_spname['all'] = self.samples
        else:
            self.group_spname = self.diff_exp.option('edger_group').get_group_spname()
            self.samples = self.diff_exp.option('edger_group').prop['sample']
        compare_column = list()
        for f in diff_files:
            if re.search(r'_edgr_stat.xls$', f):
                con_exp = f.split('_edgr_stat.xls')[0].split('_vs_')
                compare_column.append('|'.join(con_exp))
                print self.output_dir + '/' + f
                express_diff_id = api_diff_exp.test_main_table(express_id=self.option('diff_express_id'))
                api_diff_exp.add_express_diff_detail(group=con_exp, express_diff_id=express_diff_id, diff_stat_path=self.output_dir + '/' + f)
                #api_diff_exp.express_diff_up(group=con_exp, express_diff_id=self.option('diff_express_id'), diff_stat_path=self.output_dir + '/' + f )
                geneset_id = api_diff_exp.express_diff_up(group=con_exp, express_diff_id=self.option('diff_express_id'),group_id=self.option('group_id'),diff_stat_path=self.output_dir + '/' + f )
                api_diff_exp.add_geneset(geneset_id=geneset_id, diff_stat_path=self.output_dir + '/' + f)
                geneset_up_id = api_diff_exp.express_diff_up_up(group=con_exp, express_diff_id=self.option('diff_express_id'),group_id=self.option('group_id'),diff_stat_path=self.output_dir + '/' + f )
                api_diff_exp.add_geneset_up(geneset_id=geneset_up_id, diff_stat_path=self.output_dir + '/' + f)
                geneset_down_id = api_diff_exp.express_diff_up_down(group=con_exp, express_diff_id=self.option('diff_express_id'),group_id=self.option('group_id'),diff_stat_path=self.output_dir + '/' + f )
                api_diff_exp.add_geneset_down(geneset_id=geneset_down_id, diff_stat_path=self.output_dir + '/' + f)
        api_diff_exp.add_diff_summary_detail(express_id=self.option('diff_express_id'),count_path =self.output_dir + '/merge.xls')
        self.update_express_diff(table_id=self.option('diff_express_id'), compare_column=compare_column, group_detail=self.group_spname, samples=self.samples)
        self.end()

    def update_express_diff(self, table_id, compare_column, group_detail, samples):
        client = Config().mongo_client
        db_name = Config().MONGODB + '_ref_rna'
        self.logger.info(db_name)
        self.logger.info('haha')
        collection = client[db_name]['sg_ref_express_diff']
        collection.update({'_id': ObjectId(table_id)}, {'$set': {'group_detail': group_detail, 'compare_column': compare_column, 'specimen': samples}})

    def run(self):
        self.run_diff_exp()
        super(DiffExpressWorkflow, self).run()
