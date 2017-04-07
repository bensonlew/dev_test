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
            {"name": "level", "type": "string", "default": "9"},  # 输入的OTU level
            {"name": "map_detail", "type": "infile", "format": "meta.otu.group_table"},  # 输入的map_detail 示例如下(map文件后续导表)
            {"name": "group_detail", "type": "string"},
            {"name": "second_group_detail", "type": "string"},
            {"name": "add_Algorithm", "type": "string", "default": ""},
            {"name": "s", "type": "string", "default": "1"},  # OTU筛选参数
            {"name": "meta_sourcetracker_id", "type": "string"}, #主表的id
            {"name": "update_info", "type": "string"}
            # {"name": "source", "type": "string"}
            # {"A":["578da2fba4e1af34596b04ce","578da2fba4e1af34596b04cf","578da2fba4e1af34596b04d0"],"B":["578da2fba4e1af34596b04d1","578da2fba4e1af34596b04d3","578da2fba4e1af34596b04d5"],"C":["578da2fba4e1af34596b04d2","578da2fba4e1af34596b04d4","578da2fba4e1af34596b04d6"]}
            # {"name": "method", "type": "string", "default": ""}  # 聚类方式， ""为不进行聚类
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.meta_sourcetracker = self.add_tool("meta.beta_diversity.meta_sourcetracker")
        self.sort_all_samples = self.add_tool("meta.otu.sort_samples")
        self.sort_source_samples = self.add_tool("meta.otu.sort_samples")
        self.sort_sink_samples = self.add_tool("meta.otu.sort_samples")
        self.all_sort = ''
        self.source_group_file = ''
        self.sink_group_file = ''

    def check_options(self):  # 2016.12.1 zhouxuan
        if self.option('add_Algorithm') not in ['average', 'middle', 'sum', ""]:
            raise OptionError('错误的层级聚类方式：%s' % self.option('add_Algorithm'))

    def judge(self):
        """
        判断map文件是否正确，同时生成后续要用的两个小的分组文件
        :return:
        """
        self.source_group_file = os.path.join(self.work_dir, "source_group")
        self.sink_group_file = os.path.join(self.work_dir, "sink_group")
        samples = []
        sink_label = []
        with open(self.option('map_detail').prop['path'], "rb") as r, open(self.source_group_file, 'a') as e, open(self.sink_group_file, 'a') as k:
            line = r.next()
            k.write("#Sample\tsink\n")
            e.write("#Sample\tsource\n")
            for line in r:
                line = line.rstrip().split("\t")
                if line[0] not in samples:
                    samples.append(line[0])
                else:
                    raise OptionError('sink组和source组中不能存在同一个样本')
                if line[2] == 'sink':
                    k.write(line[0] + "\t" + line[1] + "\n")
                    if line[1] not in sink_label:
                        sink_label.append(line[1])
                else:
                    e.write(line[0] + "\t" + line[1] + "\n")
        if len(sink_label) == 1 or self.option('add_Algorithm') == '':
            self.run_sort_all_samples()
        else:
            self.run_sort_source_samples()

    def run_sort_all_samples(self):
        self.all_sort = "true"
        self.sort_all_samples.set_options({
            "in_otu_table": self.option("in_otu_table"),
            "group_table": self.option("map_detail")
        })
        self.sort_all_samples.on('end', self.run_meta_sourcetracker)
        self.sort_all_samples.run()

    def run_sort_source_samples(self):
        self.sort_source_samples.set_options({
            "in_otu_table": self.option("in_otu_table"),
            "group_table": self.source_group_file
        })
        self.sort_source_samples.on('end', self.run_sort_sink_samples)
        self.sort_source_samples.run()

    def run_sort_sink_samples(self):
        self.sort_sink_samples.set_options({
            "in_otu_table": self.option("in_otu_table"),
            "group_table": self.sink_group_file,
            "method": self.option('add_Algorithm')
        })
        self.sort_sink_samples.on('end', self.run_meta_sourcetracker)
        self.sort_sink_samples.run()

    def run_meta_sourcetracker(self):
        if self.all_sort == "true":
            qiime_table_path = self.reset_input_otu_table(self.sort_all_samples.option("out_otu_table").prop['path'])
        else:
            otu_table = self.add_otu_file(self.sort_source_samples.option("out_otu_table").prop['path'], self.sort_sink_samples.option("out_otu_table").prop['path'])
            qiime_table_path = self.reset_input_otu_table(otu_table)
        self.meta_sourcetracker.set_options({
            "otu_table": qiime_table_path,
            "map_table": self.option('map_detail'),
            "s": self.option("s")
        })
        self.meta_sourcetracker.on('end', self.set_db)
        self.meta_sourcetracker.run()

    def reset_input_otu_table(self, old_otu_table_path):
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
        return new_qiime_otu_table

    def add_otu_file(self, file1, file2):
        new_otu_file = os.path.join(self.work_dir, "add_file.txt")
        with open(file1, "r") as f1, open(file2, "r") as f2, open(new_otu_file, "a") as w:
            content1 = f1.readlines()
            content2 = f2.readlines()
            print "###############################"
            print len(content1)
            for i in range(0, len(content1)):
                print(content1[i])
                print(content2[i])
                content1[i] = content1[i].strip("\n")
                line2 = content2[i].rstrip("\n").split("\t")
                w.write(content1[i] + "\t" + ("\t").join(line2[1:]) + "\n")
        return new_otu_file

    def set_db(self):
        self.logger.info("正在写入mongo数据库")
        api_otu = self.api.meta_sourcetracker
        api_otu.add_sg_sourcetracker_detail(self.option("meta_sourcetracker_id"), file_path=self.meta_sourcetracker.output_dir +
                                            "/sink_predictions.txt", stdev_file_path=self.meta_sourcetracker.output_dir + "/sink_predictions_stdev.txt",
                                            name_1="sink_predictions.txt", name_2="sink_predictions_stdev.txt")
        self.end()

    def end(self):
        try:
            shutil.copy2(self.meta_sourcetracker.output_dir + "/sink_predictions.txt", self.output_dir + "/sink_predictions.txt")
            shutil.copy2(self.meta_sourcetracker.output_dir + "/sink_predictions_stdev.txt", self.output_dir + "/sink_predictions_stdev.txt")
        except Exception as e:
            self.logger.info("copying sink_predictions.txt failed{}".format(e))
            self.set_error("copying sink_predictions.txt failed{}".format(e))
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "微生物组成来源比例分析"],
            ["./sink_predictions.txt", "txt", "相对贡献度表"],
            ["./sink_predictions_stdev.txt", "txt", "相对贡献度标准差表"]
        ])
        super(MetaSourcetrackerWorkflow, self).end()

    def run(self):
        self.judge()
        super(MetaSourcetrackerWorkflow, self).run()
