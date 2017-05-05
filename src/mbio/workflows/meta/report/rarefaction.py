# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
import os


class RarefactionWorkflow(Workflow):
    """
    报告中计算稀释性曲线时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(RarefactionWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_table", "type": "infile", 'format': "meta.otu.otu_table"},  # 输入的OTU id
            {"name": "otu_id", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "indices", "type": "string"},
            {"name": "level", "type": "int"},
            {"name": "freq", "type": "int"},
            {"name": "rare_id", "type": "string"},
            {"name": "group_detail", "type": "string"}
            ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.rarefaction = self.add_tool('meta.alpha_diversity.rarefaction')

    def run(self):
        # super(EstimatorsWorkflow, self).run()
        # if self.UPDATE_STATUS_API:
        #     self.estimators.UPDATE_STATUS_API = self.UPDATE_STATUS_API
        options = {
            'otu_table': self.option('otu_table'),
            'indices': self.option('indices'),
            'freq': self.option('freq')
            }
        # print(self.option('indices'))
        self.rarefaction.set_options(options)
        self.rarefaction.on('end', self.set_db)
        self.rarefaction.run()
        self.output_dir = self.rarefaction.output_dir
        super(RarefactionWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        api_rarefaction = self.api.rarefaction
        rare_path = self.output_dir
        if os.path.isfile(rare_path):
            raise Exception("找不到报告文件夹:{}".format(rare_path))
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "稀释曲线结果目录"]
        ])
        for i in self.option("indices").split(","):
            self.logger.info(i)
            if i == "sobs":
                result_dir.add_relpath_rules([
                    ["./sobs", "文件夹", "{}指数结果输出目录".format(i)]
                ])
                result_dir.add_regexp_rules([
                    # [r".*rarefaction\.xls", "xls", "{}指数的simpleID的稀释性曲线表".format(i)]
                    [r".*rarefaction\.xls", "xls", "每个样本的{}指数稀释性曲线表".format(i)]  # modified by hongdongxuan 20170321
                ])
                # self.logger.info("{}指数的simpleID的稀释性曲线表".format(i))
            else:
                result_dir.add_relpath_rules([
                    ["./{}".format(i), "文件夹", "{}指数结果输出目录".format(i)]
                ])
                result_dir.add_regexp_rules([
                    [r".*{}\.xls".format(i), "xls", "每个样本的{}指数稀释性曲线表".format(i)]
                ])
        api_rarefaction.add_rarefaction_detail(self.option('rare_id'), rare_path)
        self.end()
