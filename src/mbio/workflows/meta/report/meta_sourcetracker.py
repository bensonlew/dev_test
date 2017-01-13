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
        new_map_detail_path = os.path.join(self.work_dir, "map_table")
        if os.path.exists(old_map_detail_path):
            b = open(old_map_detail_path, "r")
            content = b.readlines()
            first_dict = {}
            second_dict = {}
            source_sample = []
            sink_sample = []
            for f in content:
                if f.startswith("#") is False:
                    c = f.strip().split("\t")
                    first_dict[c[0]] = c[1]
                    if c[1] is self.option("source"):
                        source_sample.append(c[0])
                    elif c[1] is self.option("sink"):
                        sink_sample.append(c[0])
                    else:
                        raise OptionError('错误的一级分组方案')
                    second_dict[c[0]] = c[2]
            b.close()
            source_group_list = []
            sink_group_list = []
            all_group_list = []
            for sample in source_sample:
                source_group_list.append(second_dict[sample])
                all_group_list.append(second_dict[sample])
            for sample in sink_sample:
                sink_group_list.append(second_dict[sample])
                all_group_list.append(second_dict[sample])
            source_group_list.sort()
            sink_group_list.sort()
            all_group_list.sort()
            if len(source_group_list) + len(sink_group_list) is len(all_group_list):
                with open(new_map_detail_path, "a") as m:
                    first = "#SampleID" + "\t" + "Env" + "\t" + "SourceSink" + "\n"
                    m.write(first)
                    for sample in source_sample:
                        m.write(sample + "\t" + second_dict[sample] + "\t" + "source" + "\n")
                    for sample in sink_sample:
                        m.write(sample + "\t" + second_dict[sample] + "\t" + "sink" + "\n")
                self.map_detail_path = new_map_detail_path
            else:
                raise OptionError('错误的二级分组方案，sink组和source组的样本不能出现在同一组')
        else:
            raise OptionError('请输入正确的分组文件')
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
        api_otu = self.api.meta_sourcetracker
        api_otu.add_sg_sourcetracker_detail(self.option("meta_sourcetracker_id"), self.meta_sourcetracker.output_dir +
                                            "/sink_predictions.txt", name="sink_predictions.txt")
        api_otu.add_sg_sourcetracker_detail(self.option("meta_sourcetracker_id"), self.meta_sourcetracker.output_dir +
                                            "/sink_predictions_stdev.txt", name="sink_predictions_stdev.txt")
        self.end()

    def end(self):
        try:
            shutil.copy2(self.meta_sourcetracker.output_dir + "/sink_predictions.txt", self.output_dir + "/sink_predictions.txt")
            shutil.copy2(self.meta_sourcetracker.output_dir + "/sink_predictions_stdev.txt", self.output_dir + "/sink_predictions_stdev.txt")
        except Exception as e:
            self.logger.info("sink_predictions.txt copy success{}".format(e))
            self.set_error("sink_predictions.txt copy success{}".format(e))
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "微生物组成来源比例分析"],
            ["./sink_predictions.txt", "txt", "相对贡献度表"],
            ["./sink_predictions_stdev.txt", "txt", "相对贡献度标准差表"]
        ])
        super(MetaSourcetrackerWorkflow, self).end()

    def run(self):
        self.reset_input_table()
        super(MetaSourcetrackerWorkflow, self).run()
