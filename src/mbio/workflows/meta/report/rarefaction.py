# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
import os
####################################################
from mainapp.models.mongo.public.meta.meta import Meta
####################################################


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
            {"name": "add_Algorithm", "type": "string", "default": ""},
            {"name": "rare_id", "type": "string"},
            {"name": "group_detail", "type": "string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.rarefaction = self.add_tool('meta.alpha_diversity.rarefaction')
        ###########################################3 lines added by yiru 20170426
        self.sort_samples = self.add_tool("meta.otu.sort_samples")
        group_table_path = os.path.join(self.work_dir, "group_table.xls")
        self.group_table_path = Meta().group_detail_to_table(self.option("group_detail"), group_table_path)
        ##############################################
        


    ##################################################3 functions added by yiru 20170426
    def check_options(self):
        if self.option('add_Algorithm') not in ['average', 'middle', ""]:
            raise OptionError('错误的样本求和方式：%s' % self.option('add_Algorithm'))

    def run_sort_samples(self):
        self.sort_samples.set_options({
            "in_otu_table": self.option("otu_table").prop['path'],
            # "group_table": self.option("group_detail"),
            "group_table": self.group_table_path,
            "method": self.option("add_Algorithm")
        })
        self.sort_samples.on("end",self.run_rarefaction)    
        self.sort_samples.run()

    def run_rarefaction(self):
        self.rarefaction.set_options({
            'otu_table': self.sort_samples.option("out_otu_table"),
            'indices': self.option('indices'),
            'freq': self.option('freq')
        })
        self.rarefaction.on('end', self.set_db)
        self.output_dir = self.rarefaction.output_dir
        self.rarefaction.run()
    ###################################################

    def run(self): ###run function edited by yiru 20170426
        # super(EstimatorsWorkflow, self).run()
        # if self.UPDATE_STATUS_API:
        #     self.estimators.UPDATE_STATUS_API = self.UPDATE_STATUS_API
        self.run_sort_samples()
        super(RarefactionWorkflow, self).run()

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        api_rarefaction = self.api.rarefaction
        rare_path = self.output_dir
        if os.path.isfile(rare_path):
            raise Exception("找不到报告文件夹:{}".format(rare_path))
        api_rarefaction.add_rarefaction_detail(self.option('rare_id'), rare_path)
        self.end()

    def end(self):
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
        super(RarefactionWorkflow, self).end()  
