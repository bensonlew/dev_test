# -*- coding: utf-8 -*-
# __author__ = 'zengjing'

"""16s功能预测分析"""

from biocluster.workflow import Workflow
import os
import re


class FunctionPredictWorkflow(Workflow):
    """
    报告中调用16s功能预测分析时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(FunctionPredictWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_table", "type": "string", "default": "none"},
            {"name": "update_info", "type": "string"},
            {"name": "predict_id", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "group_detail", "type": "string"},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.function_predict = self.add_tool("meta.function_predict")
        self.output_dir= self.function_predict.output_dir

    def run_function_predict(self):
        files = self.option("otu_table").split(',')
        options = {
            "otu_reps.fasta": files[1],
            "otu_table.xls": files[0],
            "db": "both",
        }
        self.function_predict.set_options(options)
        self.function_predict.on("end", self.set_db)
        self.function_predict.run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "dir", "结果输出目录"],
            ["./cog/", "dir", "cog功能预测分析输出目录"],
            ["./KEGG/", "dir", "kegg功能预测分析输出目录"]
        ])
        result_dir.add_regexp_rules([
            [r"/cog/cog.category.function.xls", "xls", ""],
            [r"/cog/cog.descrip.table.xls", "xls", ""],
            [r"/KEGG/predictions_ko.xls", "xls", ""],
            [r"/KEGG/predictions_ko.L1.xls", "xls", ""],
            [r"/KEGG/predictions_ko.L2.xls", "xls", ""],
            [r"/KEGG/predictions_ko.L3.xls","xls", ""],
            [r"/KEGG/kegg.pathway.profile.xls", "xls", ""],
            [r"/KEGG/kegg.enzyme.profile.xls", "xls", ""]
        ])
        super(FunctionPredictWorkflow, self).end()

    def set_db(self):
        """
        将结果保存到mongo数据库中
        """
        self.logger.info("运行set_db")
        api_fun = self.api.function_predict
        table_path = self.option("otu_table").split(',')[0]
        sample_path = self.output_dir + '/cog/cog.descrip.table.xls'
        function_path = self.output_dir + '/cog/cog.category.function.xls'
        prediction_id = self.option("predict_id")
        if os.path.exists(sample_path) and os.path.exists(function_path):
            api_fun.add_cog_function_prediction(prediction_id=prediction_id, sample_path=sample_path, function_path=function_path, table_path=table_path)
        else:
            raise Exception("找不到COG功能预测的结果文件！")
        kegg_path = self.output_dir + '/KEGG'
        maps_path = self.output_dir + '/pics'
        if os.path.exists(kegg_path) and os.path.exists(maps_path):
            api_fun.add_kegg_function_prediction(prediction_id=prediction_id, kegg_path=kegg_path, maps_path=maps_path, table_path=table_path)
        else:
            raise Exception("找不到KEGG功能预测的结果文件!")
        self.end()

    def run(self):
        self.run_function_predict()
        super(FunctionPredictWorkflow, self).run()
