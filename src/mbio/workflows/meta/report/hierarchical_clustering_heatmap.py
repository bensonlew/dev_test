# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan '

"""聚类heatmap图模块"""
import os
import json
import datetime
import shutil
import re
from biocluster.core.exceptions import OptionError
from biocluster.workflow import Workflow
from mainapp.models.mongo.public.meta.meta import Meta


class HierarchicalClusteringHeatmapWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(HierarchicalClusteringHeatmapWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "in_otu_table", "type": "infile", "format": "meta.otu.otu_table"},  # 输入的OTU表
            {"name": "input_otu_id", "type": "string"},  # 输入的OTU id
            {"name": "level", "type": "string", "default": "9"},  # 输入的OTU level
            {"name": "group_detail", "type": "string"},  # 输入的group_detail 示例如下
            # {"A":["578da2fba4e1af34596b04ce","578da2fba4e1af34596b04cf","578da2fba4e1af34596b04d0"],"B":["578da2fba4e1af34596b04d1","578da2fba4e1af34596b04d3","578da2fba4e1af34596b04d5"],"C":["578da2fba4e1af34596b04d2","578da2fba4e1af34596b04d4","578da2fba4e1af34596b04d6"]}
            {"name": "species_number", "type": "string", "default": ""},  # 物种数目，默认全部物种
            {"name": "method", "type": "string", "default": ""},  # 物种层次聚类方式，默认不聚类
            {"name": "sample_method", "type": "string", "default": ""},  # 样本层次聚类方式，默认不聚类
            {"name": "add_Algorithm", "type": "string", "default": ""},  # 分组样本求和算法，默认不求和
            {"name": "update_info", "type": "string"},
            {"name": "main_id", "type": "string"},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.sort_samples = self.add_tool("meta.otu.sort_samples")
        self.matrix = self.add_tool("meta.beta_diversity.distance_calc")
        self.sample_matrix = self.add_tool("meta.beta_diversity.distance_calc") # 20161206 2 lines
        self.sample_hcluster = self.add_tool('meta.beta_diversity.hcluster')
        self.hcluster = self.add_tool('meta.beta_diversity.hcluster')
        group_table_path = os.path.join(self.work_dir, "group_table.xls")
        self.group_table_path = Meta().group_detail_to_table(self.option("group_detail"), group_table_path)
        self.s2_file_path = ""  #

    def check_options(self):
        if self.option('method') not in ['average', 'single', 'complete', ""]:
            raise OptionError('错误的物种层次聚类方式：%s' % self.option('method'))
        if self.option('sample_method') not in ['average', 'single', 'complete', ""]:
            raise OptionError('错误的样本层次聚类方式：%s' % self.option('sample_method'))
        if self.option('add_Algorithm') not in ['sum', 'average', 'middle', ""]:
            raise OptionError('错误的样本求和方式：%s' % self.option('add_Algorithm'))
        # if (self.option("method") != "" and self.option("species_number") != ("" and "all")):
        if self.option("method") != "" :
            if (self.option("species_number") != "" and self.option("species_number") != "all"):
                if int(self.option("species_number")) == 1:
                    raise OptionError('物种聚类的个数不能为：%s' % self.option('species_number'))
        # if self.option("sample_method") != "":
        #     print(self.group_table_path)
        #     sample_n = open(self.group_table_path, "r")
        #     content = sample_n.readlines()
        #     if len(content) == 2:
        #         raise OptionError('样本聚类的个数不能为：1' )
        #     if self.option("add_Algorithm") != "":
        #         sample_ = []
        #         sample_n2 = open(self.group_table_path, "r")
        #         content = sample_n2.readlines()
        #         for f in content:
        #             f = f.strip("\n")
        #             arr = f.strip().split("\t")
        #             if arr[0] != "#sample":
        #                 if arr[1] not in sample_:
        #                     sample_.append(arr[1])
        #         if len(sample_) == 1:
        #             raise OptionError('当计算分组丰度并且进行样本聚类的分析时,样本分组不能为1' )

    def get_species(self):
        global new_otu_file_path
        old_otu_file_path = self.option("in_otu_table").prop["path"]

        if self.option("sample_method") != "":  # 判断是否能做聚类
            print(self.group_table_path)
            sample_n = open(self.group_table_path, "r")
            content = sample_n.readlines()
            if len(content) == 2:
                raise OptionError('样本聚类的个数不能为：1' )
            if self.option("add_Algorithm") != "":
                sample_ = []
                sample_n2 = open(self.group_table_path, "r")
                content = sample_n2.readlines()
                for f in content:
                    f = f.strip("\n")
                    arr = f.strip().split("\t")
                    if arr[0] != "#sample":
                        if arr[1] not in sample_:
                            sample_.append(arr[1])
                if len(sample_) == 1:
                    raise OptionError('当计算分组丰度并且进行样本聚类的分析时,样本分组不能为1' )

        # if self.option("species_number") == "all":
        #     self.s2_file_path = os.path.join(self.work_dir, "s_otu_table.xls")
        #     s1 = open(old_otu_file_path, 'r')
        #     contento = s1.readlines()
        #     for n in contento:
        #         n = n.strip("\n")
        #         brr = n.strip().split(" ")
        #         if brr[0] == "OTU":
        #             with open(self.s2_file_path, "a") as w:
        #                 w.write(brr[0] + " " + brr[1] + "\n")
        #         else:
        #             with open(self.s2_file_path, "a") as w:
        #                 w.write(brr[-1] + "\n")
        #     new_otu_file_path = self.s2_file_path

        self.s2_file_path = os.path.join(self.work_dir, "s_otu_table.xls")
        s1 = open(old_otu_file_path, 'r')
        contento = s1.readlines()
        for n in contento:
            n = n.strip("\n")
            brr = n.strip().split(" ")
            if brr[0] == "OTU":
                with open(self.s2_file_path, "a") as w:
                    w.write(brr[0] + " " + brr[1] + "\n")
            else:
                with open(self.s2_file_path, "a") as w:
                    w.write(brr[-1] + "\n")
        new_otu_file_path = self.s2_file_path

        middle_otu_file_path = os.path.join(self.work_dir, "middle_otu_table.xls")
        old_file = open(old_otu_file_path, 'r')
        content = old_file.readlines()
        all = len(open(old_otu_file_path).readlines())  # print(all)  # test
        list1 = []
        for f in content:
            f = f.strip("\n")
            arr = f.strip().split("\t")
            number = len(arr)
            reads_number = 0
            if arr[0] == "OTU ID": # 第一行copy到middle中
                 first_line = f
                 with open(middle_otu_file_path, "a") as w:
                     w.write(f + ":" + "a" +"\n")
            else:  # if arr[0] != "OTU ID":
                for i in range(1,number):
                    reads_number = reads_number + int(arr[i])
                list1.append(int(reads_number))
                linecontent = f + ":" + str(reads_number) + "\n"
                with open(middle_otu_file_path, "a") as w:
                    w.write(linecontent)
        list1.sort()
        list1.reverse()  # print(list1)

        list2 = [] # 放入sum值 为重复做准备
        if (self.option("species_number") != "all" and self.option("species_number") != ""):
            species_nu = int(self.option("species_number"))
            if species_nu >= all:
                new_otu_file_path = self.s2_file_path
            else:
                new_otu_file_path = os.path.join(self.work_dir, "new_otu_table.xls")
                middle_file = open(middle_otu_file_path, "r")
                content2 = middle_file.readlines()
                with open(new_otu_file_path, "a") as w:
                    w.write(first_line + "\n")

                t = 0
                for i in range(species_nu):  # print(list1[i])
                    if list1[i] in list2:  # print(list1[i])
                        continue
                    for line in content2:
                        line = line.strip("\n")
                        arr_2 = line.strip().split(":")
                        if arr_2[1] == str(list1[i]):
                            f = arr_2[0].strip().split(" ")
                            linecontent_2 = f[-1] + "\n"
                            # linecontent_2 = arr_2[0] + "\n"
                            list2.append(list1[i])  # 为重复的sum判断做准备
                            with open(new_otu_file_path, "a") as w:
                                w.write(linecontent_2)
                                t = t + 1
                                if t == species_nu:
                                    break


    def run_sort_samples(self):
        self.sort_samples.set_options({
            "in_otu_table": new_otu_file_path,
            # "group_table": self.option("group_detail"),
            "group_table": self.group_table_path,
            "method": self.option("add_Algorithm")
        })
        if self.option("method") == "":
            if self.option("sample_method") == "":
                self.sort_samples.on("end", self.set_db)
            else:
                self.sort_samples.on("end", self.run_sample_matrix)
        else:
            self.sort_samples.on("end", self.run_matrix)
        # self.output_dir = self.sort_samples.output_dir
        self.sort_samples.run()

    def run_matrix(self):
        trans_otu = os.path.join(self.work_dir, "otu.trans")
        self.sort_samples.option("out_otu_table").transposition(trans_otu)
        self.matrix.set_options({
            "method": "bray_curtis",
            "otutable": trans_otu
        })
        self.matrix.on('end', self.run_cluster)
        self.matrix.run()

    def run_cluster(self):
        options = {
            "dis_matrix": self.matrix.option('dis_matrix'),
            "linkage": self.option("method")
        }
        self.hcluster.set_options(options)
        if self.option("sample_method") != "":
            self.hcluster.on('end', self.run_sample_matrix)
        else:
            self.hcluster.on('end', self.set_db)
        self.hcluster.run()

    def run_sample_matrix(self): #20161206
        self.logger.info("正在进行样本距离计算")
        options = {
            "method": "bray_curtis",
            "otutable": self.sort_samples.option("out_otu_table")
        }
        self.sample_matrix.set_options(options)
        self.sample_matrix.on('end', self.run_sample_hcluster)
        self.logger.info("样本距离计算结束")
        self.sample_matrix.run()
    def run_sample_hcluster(self):
        self.logger.info("正在进行样本聚类计算")
        options = {
            "dis_matrix": self.sample_matrix.option('dis_matrix'),
            "linkage": self.option("sample_method")
        }
        self.sample_hcluster.set_options(options)
        self.sample_hcluster.on('end', self.set_db)
        self.logger.info("样本聚类计算结束")
        self.sample_hcluster.run()


    def set_db(self):
        sample_tree = ""
        species_tree = ""
        sample_list = []
        species_list = []
        self.logger.info("正在写入mongo数据库")
        # myParams = json.loads(self.sheet.params)
        if self.option("method") != "":
            species_tree_path = self.hcluster.option("newicktree").prop['path']
            if os.path.exists(species_tree_path):
                with open(species_tree_path, "r") as f:
                    species_tree = f.readline().strip()
                    raw_samp = re.findall(r'([(,]([\[\]\.\;\'\"\ 0-9a-zA-Z_-]+?):[0-9])', species_tree)
                    species_list = [i[1] for i in raw_samp]
        if self.option("sample_method") != "":
            sample_tree_path = self.sample_hcluster.option("newicktree").prop['path']
            if os.path.exists(sample_tree_path):
                with open(sample_tree_path, "r") as f:
                    sample_tree = f.readline().strip()
                    raw_samp = re.findall(r'([(,]([\[\]\.\;\'\"\ 0-9a-zA-Z_-]+?):[0-9])', sample_tree)
                    sample_list = [i[1] for i in raw_samp]
        # else:
            # if self.option("add_Algorithm") != "":
        if (self.option("add_Algorithm") != "" and self.option("sample_method") == ""):
            sample_name = open(self.group_table_path, "r")
            content = sample_name.readlines()
            for f in content:
                f = f.strip("\n")
                arr = f.strip().split("\t")
                if arr[0] != "#sample":
                    if arr[1] not in sample_list:
                        sample_list.append(arr[1])
        api_otu = self.api.hierarchical_clustering_heatmap
        # new_otu_id = api_otu.add_sg_hc_heatmap(self.sheet.params, self.option("input_otu_id"), None,
        #                                        sample_tree = sample_tree, sample_list = sample_list,
        #                                        species_tree = species_tree, species_list = species_list)
        api_otu.add_sg_hc_heatmap_detail(self.sort_samples.option("out_otu_table").prop["path"], self.option("main_id"), self.option("input_otu_id"),
                                         sample_tree=sample_tree, sample_list=sample_list,
                                         species_tree = species_tree, species_list = species_list)
        # self.add_return_mongo_id("sg_hc_heatmap", self.option("main_id"))
        self.end()

    def end(self):
        shutil.copy(self.sort_samples.output_dir + "/out_otu.xls", self.output_dir + "/out_otu.xls")
        if os.path.exists(self.sample_hcluster.output_dir + "/hcluster.tre"):
            shutil.copy(self.sample_hcluster.output_dir + "/hcluster.tre", self.output_dir + "/sample_hcluster.tre")
        if os.path.exists(self.hcluster.output_dir + "/hcluster.tre"):
            shutil.copy(self.hcluster.output_dir + "/hcluster.tre", self.output_dir + "/species_hcluster.tre")
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "HierarchicalClusteringHeatmap分析结果输出目录"],
            ["./out_otu.xls", "xls", "HierarchicalClusteringHeatmap分析可视化结果数据表"],
            ["./sample_hcluster.tre", "tre", "样本聚类树"],
            ["./species_hcluster.tre", "tre", "物种聚类树"]
        ])
        super(HierarchicalClusteringHeatmapWorkflow, self).end()

    def run(self):
        self.get_species()
        self.run_sort_samples()
        super(HierarchicalClusteringHeatmapWorkflow, self).run()
