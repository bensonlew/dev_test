# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan'

"""微生物组成来源比例分析模块"""
import os
import json
import shutil
import datetime
from biocluster.core.exceptions import OptionError
from biocluster.workflow import Workflow
from mainapp.models.mongo.public.meta.meta import Meta


class MetaSourcetrackerWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(MetaSourcetrackerWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_otu_table", "type": "infile", "format": "meta.otu.otu_table"},  # 输入的OTU表
            # {"name": "input_otu_id", "type": "string"},  # 输入的OTU id
            {"name": "level", "type": "string", "default": "9"},  # 输入的OTU level
            {"name": "map_detail", "type": "infile", "format": "meta.otu.group_table"},  # 输入的map_detail 示例如下(map文件后续导表)
            {"name": "meta_sourcetracker_id", "type": "string"}, #主表的id
            {"name": "update_info", "type": "string"},
            {"name": "s", "type": "string", "default": "1"},  #OTU筛选参数
            {"name": "sink", "type": "string"},
            {"name": "source", "type": "string"}
            # {"A":["578da2fba4e1af34596b04ce","578da2fba4e1af34596b04cf","578da2fba4e1af34596b04d0"],"B":["578da2fba4e1af34596b04d1","578da2fba4e1af34596b04d3","578da2fba4e1af34596b04d5"],"C":["578da2fba4e1af34596b04d2","578da2fba4e1af34596b04d4","578da2fba4e1af34596b04d6"]}
            # {"name": "method", "type": "string", "default": ""}  # 聚类方式， ""为不进行聚类
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.meta_sourcetracker = self.add_tool("meta.beta_diversity.meta_sourcetracker")
        self.a = ''
        self.spe_name = ''
        self.number = ''
        self.qiime_table_path = ''
        self.map_detail_path = ''
        # group_table_path = os.path.join(self.work_dir, "group_table.xls")
        # self.group_table_path = Meta().group_detail_to_table(self.option("group_detail"), group_table_path)

    def reset_input_table(self):
        old_otu_table_path = self.option("in_otu_table").prop['path']
        new_qiime_otu_table = os.path.join(self.work_dir, "otu_table.txt")
        with open(new_qiime_otu_table, "a") as n:
            firstline = "# QIIME-formatted OTU table" + "\n"
            print (firstline)
            n.write(firstline)
        if os.path.exists(old_otu_table_path):
            a = open(old_otu_table_path, "r")
            content = a.readlines()
            for f in content:
                if f.startswith("OTU ID") == True:
                    line = "#" + f
                else:
                    line = f
                with open(new_qiime_otu_table, "a") as n:
                    n.write(line)
            a.close()
        self.qiime_table_path = new_qiime_otu_table

        old_map_detail_path = self.option("map_detail").prop['path']  # 检查group文件并根据group文件
        if os.path.exists(old_map_detail_path):
            b = open(old_map_detail_path, "r")
            content = b.readlines()

        new_map_detail_path = os.path.join(self.work_dir, "map_table")

        self.run_meta_sourcetracker()

    def run_meta_sourcetracker(self):
        self.meta_sourcetracker.set_options({
            "otu_table": self.qiime_table_path,
            "map_table": self.map_detail_path,
            "s": self.option("s")
        })
        self.meta_sourcetracker.on('end', self.set_db)
        self.meta_sourcetracker.run()


    def set_db(self):
        self.logger.info("正在写入mongo数据库")
        # api_otu = self.api.enterotyping_db
        # new_id = api_otu.add_sg_enterotyping(self.sheet.params, self.option("input_otu_id"))
        # api_otu.add_sg_enterotyping_detail(new_id, self.enterotyping.output_dir + "/ch.txt", x = "x", y = "y", name = "ch.txt")
        # api_otu.add_sg_enterotyping_detail(new_id, self.enterotyping.output_dir + "/cluster.txt",x = "sample_name", y = "enterotyping_group", name = "cluster.txt")
        # api_otu.add_sg_enterotyping_detail(new_id, self.plot_enterotyping.output_dir + "/circle.txt", x = "x", y = "y", name = "circle.txt", detail_name = "circle_name")
        # api_otu.add_sg_enterotyping_detail(new_id, self.plot_enterotyping.output_dir + "/point.txt", x="x", y="y",
        #                                    name="point.txt", detail_name="sample_name")
        # for i in range(1, int(self.number)):
        #     api_otu.add_sg_enterotyping_detail_cluster(new_id, self.enterotyping.output_dir + "/" + str(i) + ".cluster.txt", name = str(i) + ".cluster.txt")
        # api_otu.add_sg_enterotyping_detail_summary(new_id, self.plot_enterotyping.output_dir + "/summary.txt",
        #                                            name="summary.txt")
        # self.add_return_mongo_id("sg_enterotyping", new_id)
        self.end()

    # def end(self):
    #     try:
    #         shutil.copy2(self.plot_enterotyping.output_dir + "/summary.txt", self.output_dir + "/summary.txt")
    #         shutil.copytree(self.enterotyping.output_dir, self.output_dir + "/enterotyping")
    #     except Exception as e:
    #         self.logger.info("summary.txt copy success{}".format(e))
    #         self.set_error("summary.txt copy success{}".format(e))
    #     result_dir = self.add_upload_dir(self.output_dir)
    #     result_dir.add_relpath_rules([
    #         [".", "", "样本菌群分型分析结果输出目录"],
    #         ["./summary.txt", "txt", "summary数据表"],
    #         ["./enterotyping", "dir", "分型数据文件夹"],
    #         ["./enterotyping/ch.txt", "txt", "CH指数数据表"],
    #         ["./enterotyping/cluster.txt", "txt", "cluster数据表"]
    #     ])
    #     result_dir.add_regexp_rules([
    #         ["enterotyping/.+\cluster.txt$", "txt", "分型后各组数据表"]
    #     ])
    #     super(MetaSourcetrackerWorkflow, self).end()

    def run(self):
        self.reset_input_table()
        super(MetaSourcetrackerWorkflow, self).run()
