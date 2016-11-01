# -*- coding: utf-8 -*-
# __author__ = 'yuguo'

"""otu样本序列数抽平"""
from biocluster.workflow import Workflow
import os
import shutil
from mainapp.models.mongo.public.meta.meta import Meta


class OtuSubsampleWorkflow(Workflow):

    """
    报告中调用otu抽平时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(OtuSubsampleWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_otu_table", "type": "infile", "format": "meta.otu.otu_table"},  # 输入的OTU表
            {"name": "input_otu_id", "type": "string"},  # 输入的OTU id
            {"name": "filter_json", "type": "string", "default": ""},  # 输入的json文件
            {"name": "size", "type": "string", "default": "min"},
            {"name": "group_detail", "type": "string"},
            {"name": "level", "type": "string", "default": "9"},
            {"name": "output_otu_id", "type": "string"}  # 结果的otu id
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.filter_otu = self.add_tool("meta.otu.filter_otu")
        self.sort_samples = self.add_tool("meta.otu.sort_samples")
        self.subsample = self.add_tool("meta.otu.sub_sample")
        group_table_path = os.path.join(self.work_dir, "group_table.xls")
        self.group_table_path = Meta().group_detail_to_table(self.option("group_detail"), group_table_path)

    def run_filter_otu(self):
        if self.option("filter_json") != "":
            self.filter_otu.set_options({
                "in_otu_table": self.option("in_otu_table"),
                "filter_json": self.option("filter_json")
            })
            self.filter_otu.on("end", self.run_sort_samples)
            self.filter_otu.run()
        else:
            self.run_sort_samples()

    def run_sort_samples(self):
        if self.option("filter_json") == "":
            self.sort_samples.set_options({
                "in_otu_table": self.option("in_otu_table"),
                "group_table": self.group_table_path
            })
        else:
            self.sort_samples.set_options({
                "in_otu_table": self.filter_otu.option("out_otu_table"),
                "group_table": self.group_table_path
            })
        if self.option("size") != "":
            self.sort_samples.on("end", self.run_subsample)
        else:
            self.sort_samples.on("end", self.set_db)
        self.sort_samples.run()

    def run_subsample(self):
        self.subsample.set_options({
            "in_otu_table": self.sort_samples.option("out_otu_table"),
            "size": self.option("size")
        })
        self.subsample.on('end', self.set_db)
        self.subsample.run()

    def set_db(self):
        """
        保存结果otu表到mongo数据库中
        """
        final_file = os.path.join(self.output_dir, "otu_taxon.subsample.xls")
        if self.option("size") != "":
            shutil.copy2(self.subsample.option("out_otu_table").prop["path"], final_file)
        else:
            shutil.copy2(self.sort_samples.option("out_otu_table").prop["path"], final_file)
        api_otu = self.api.sub_sample
        output_otu_id = api_otu.add_sg_otu(self.sheet.params, self.option("size"), self.option("input_otu_id"))
        if not os.path.isfile(final_file):
            raise Exception("找不到报告文件:{}".format(final_file))
        self.logger.info("开始讲信息导入sg_otu_detail表和sg_otu_specimen表中")
        api_otu.add_sg_otu_detail(final_file, self.option("input_otu_id"), output_otu_id)
        api_otu.add_sg_otu_detail_level(final_file, output_otu_id, self.option("level"))
        self.add_return_mongo_id("sg_otu", output_otu_id)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            ['\.subsample\.', 'meta.otu.otu_table', "抽平后的otu表格"]
        ])
        super(OtuSubsampleWorkflow, self).end()

    def run(self):
        self.run_filter_otu()
        super(OtuSubsampleWorkflow, self).run()
