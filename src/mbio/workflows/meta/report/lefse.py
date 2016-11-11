# -*- coding: utf-8 -*-
# __author__ = 'qiuping'

"""lefse分析"""

from biocluster.workflow import Workflow
import os


class LefseWorkflow(Workflow):
    """
    报告中调用lefse分析时使用
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(LefseWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "otu_file", "type": "infile", 'format': "meta.otu.otu_table"},
            {"name": "group_file", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "group_detail", "type": "string"},
            {"name": "second_group_detail", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "lda_filter", "type": "float", "default": 2.0},
            {"name": "strict", "type": "int", "default": 0},
            {"name": "group_name", "type": "string"},
            {"name": "lefse_id", "type": "string"},
            {"name": "start_level", "type": "int", "default": 1},
            {"name": "end_level", "type": "int", "default": 8},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.lefse = self.add_tool("statistical.lefse")
        self.logger.info(self.option("group_name"))

    def run_lefse(self):
        options = {
            "lefse_input": self.option("otu_file"),
            "lefse_group": self.option("group_file"),
            "lda_filter": self.option("lda_filter"),
            "strict": self.option("strict"),
            "lefse_gname": self.option("group_name"),
            "start_level": self.option("start_level"),
            "end_level": self.option("end_level"),
        }

        self.lefse.set_options(options)
        self.lefse.on("end", self.set_db)
        self.output_dir = self.lefse.output_dir
        self.lefse.run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "lefse分析结果输出目录"],
            ["./lefse_LDA.cladogram.png", "png", "lefse分析cladogram结果图片"],
            ["./lefse_LDA.png", "png", "lefse分析LDA图片"],
            ["./lefse_LDA.xls", "xls", "lefse分析lda数据表"]
        ])
        super(LefseWorkflow, self).end()

    def set_db(self):
        """
        保存两组比较分析的结果表保存到mongo数据库中
        """
        api_lefse = self.api.stat_test
        lefse_path = self.output_dir + '/lefse_LDA.xls'
        lda_png_path = self.output_dir + '/lefse_LDA.png'
        lda_cladogram_path = self.output_dir + '/lefse_LDA.cladogram.png'
        if not os.path.isfile(lefse_path):
            raise Exception("找不到报告文件:{}".format(lefse_path))
        if not os.path.isfile(lda_png_path):
            raise Exception("找不到报告文件:{}".format(lda_png_path))
        if not os.path.isfile(lda_cladogram_path):
            raise Exception("找不到报告文件:{}".format(lda_cladogram_path))
        api_lefse.add_species_difference_lefse_detail(file_path=lefse_path, table_id=self.option("lefse_id"))
        api_lefse.update_species_difference_lefse(lda_png_path, lda_cladogram_path, self.option("lefse_id"))
        self.end()

    def run(self):
        self.run_lefse()
        super(LefseWorkflow, self).run()
