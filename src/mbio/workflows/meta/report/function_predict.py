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
            {"name": "update_info", "type": "string", "default": "none"},
            {"name": "predict_id", "type": "string", "default": "none"},
            {"name": "group_id", "type": "string", "default": "none"},
            {"name": "otu_id", "type": "string", "default": "none"},
            {"name": "group_detail", "type": "string", "default": "none"},
            {"name": "group_method", "type": "string", "default": "none"},
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
            [".", "dir", "16s功能预测结果目录"],
            ["./COG/", "dir", "COG功能预测结果目录"],
            ["./KEGG/", "dir", "kegg功能预测结果目录"],
            ["./KEGG/Pathway_pdf", "dir", "kegg通路图pdf格式"],
            ["./KEGG/Pathway_png", "dir", "kegg通路图png格式"],
            [r"./COG/cog.category.function.xls", "xls", "功能代码在各个样本中的丰度统计"],
            [r"./COG/cog.descrip.table.xls", "xls", "cog在各个样本中的丰度表及描述信息"],
            [r"./COG/predictions_cog.biom", "biom", "biom格式cog丰度表"],
            [r"./COG/predictions_cog.xls", "xls", "cog在各个样品中的丰度"],
            [r"./COG/predictions_cog.txt", "txt", "txt格式cog丰度表"],
            [r"./KEGG/predictions_ko.xls", "xls", "ko在各个样品中的丰度表"],
            [r"./KEGG/predictions_ko.biom", "biom", "biom格式ko丰度表"],
            [r"./KEGG/predictions_ko.txt", "txt", "txt格式ko丰度表"],
            [r"./KEGG/predictions_ko.L1.xls", "xls", "代谢通路level 1丰度表"],
            [r"./KEGG/predictions_ko.L1.biom", "biom", "biom格式代谢通路level 1丰度表"],
            [r"./KEGG/predictions_ko.L1.txt", "txt", "txt格式代谢通路level 1丰度表"],
            [r"./KEGG/predictions_ko.L2.xls", "xls", "代谢通路level 2丰度表"],
            [r"./KEGG/predictions_ko.L2.biom", "biom", "biom格式代谢通路level 2丰度表"],
            [r"./KEGG/predictions_ko.L2.txt", "txt", "txt格式代谢通路level 2丰度表"],
            [r"./KEGG/predictions_ko.L3.xls", "xls", "代谢通路level 3丰度表"],
            [r"./KEGG/predictions_ko.L3.biom", "biom", "biom格式代谢通路level 3丰度表"],
            [r"./KEGG/predictions_ko.L3.txt", "txt", "txt格式代谢通路level 3丰度表"],
            [r"./KEGG/kegg.pathway.profile.xls", "xls", "各个样品的pathway丰度统计"],
            [r"./KEGG/kegg.enzyme.profile.xls", "xls", "各个样品的Enzyme丰度统计"],
        ])
        super(FunctionPredictWorkflow, self).end()

    def set_db(self):
        """
        将结果保存到mongo数据库中
        """
        self.logger.info("运行set_db")
        api_fun = self.api.function_predict
        sample_path = self.option("otu_table").split(',')[0]
        table_path= self.output_dir + '/COG/cog.descrip.table.xls'
        function_path = self.output_dir + '/COG/cog.category.function.xls'
        prediction_id = self.option("predict_id")
        if os.path.exists(sample_path):
            api_fun.update_specimen(sample_path=sample_path, prediction_id=prediction_id)
        if os.path.exists(table_path) and os.path.exists(function_path):
            api_fun.add_cog_function(prediction_id=prediction_id, sample_path=sample_path, function_path=function_path)
            api_fun.add_cog_specimen(prediction_id=prediction_id, sample_path=sample_path, group_method=self.option("group_method"), table_path=table_path)
        else:
            raise Exception("找不到COG功能预测的结果文件！")
        kegg_path = self.output_dir + '/KEGG'
        new_maps = kegg_path + '/Pathway_png'
        if os.path.exists(kegg_path) and os.path.exists(new_maps):
            api_fun.add_kegg_specimen(prediction_id=prediction_id, kegg_path=kegg_path, maps_path=new_maps, sample_path=sample_path)
            api_fun.add_kegg_level(prediction_id=prediction_id, kegg_path=kegg_path, sample_path=sample_path)
        else:
            raise Exception("找不到KEGG功能预测的结果文件!")
        self.end()

    def run(self):
        self.run_function_predict()
        super(FunctionPredictWorkflow, self).run()
