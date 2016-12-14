# -*- coding: utf-8 -*-
# __author__ = 'zhouxuan '

"""聚类heatmap图模块"""
import os
import json
import datetime
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
            {"name": "species_number", "type": "string", "default": "all"},  # 物种数目，默认全部物种
            {"name": "method", "type": "string", "default": ""},  # 物种层次聚类方式，默认不聚类
            {"name": "sample_method", "type": "string", "default": ""},  # 样本层次聚类方式，默认不聚类
            {"name": "add_Algorithm", "type": "string", "default": ""},  # 分组样本求和算法，默认不求和
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

    def check_options(self):
        if self.option('method') not in ['average', 'single', 'complete', ""]:
            raise OptionError('错误的物种层次聚类方式：%s' % self.option('method'))
        if self.option('sample_method') not in ['average', 'single', 'complete', ""]:
            raise OptionError('错误的样本层次聚类方式：%s' % self.option('sample_method'))
        if self.option('add_Algorithm') not in ['sum', 'average', 'middle', ""]:
            raise OptionError('错误的样本求和方式：%s' % self.option('add_Algorithm'))

    def get_species(self):
        global new_otu_file_path
        old_otu_file_path = self.option("in_otu_table").prop["path"]
        if self.option("species_number") == "all":
            new_otu_file_path = old_otu_file_path
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
        if self.option("species_number") != "all":
            species_nu = int(self.option("species_number"))
            if species_nu >= all:
                new_otu_file_path = old_otu_file_path
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
                            linecontent_2 = arr_2[0] + "\n"
                            list2.append(list1[i])  # 为重复的sum判断做准备
                            with open(new_otu_file_path, "a") as w:
                                w.write(linecontent_2)
                                t = t + 1
                                if t == species_nu:
                                    break


    def run_sort_samples(self):
        self.sort_samples.set_options({
            "in_otu_table": new_otu_file_path,
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
        self.output_dir = self.sort_samples.output_dir
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
        self.logger.info("正在写入mongo数据库")
        newick_id = ""
        myParams = json.loads(self.sheet.params)
        if self.option("method") != "":
            api_heat_cluster = self.api.heat_cluster  #调用画图热图的
            name = "heat_cluster_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            newick_id_species = api_heat_cluster.create_newick_table(self.sheet.params, self.option("method"), myParams["otu_id"], name)
            self.hcluster.option("newicktree").get_info()
            api_heat_cluster.update_newick(self.hcluster.option("newicktree").prop['path'], newick_id_species)
            self.add_return_mongo_id("sg_newick_tree", newick_id_species, "", False)
        if self.option("sample_method") != "":
            api_sample_cluster = self.api.heat_cluster
            name = "sample_cluster_" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            newick_id_sample = api_sample_cluster.create_newick_table(self.sheet.params, self.option("sample_method"), myParams["otu_id"], name)
            self.sample_hcluster.option("newicktree").get_info()
            api_sample_cluster.update_newick(self.sample_hcluster.option("newicktree").prop['path'], newick_id_sample)
            self.add_return_mongo_id("sg_newick_tree", newick_id_sample, "", False)

        api_otu = self.api.hierarchical_clustering_heatmap
        new_otu_id = api_otu.add_sg_hc_heatmap(self.sheet.params, self.option("input_otu_id"), None, newick_id_sample, newick_id_species)
        api_otu.add_sg_hc_heatmap_detail(self.sort_samples.option("out_otu_table").prop["path"], new_otu_id, self.option("input_otu_id"))
        self.add_return_mongo_id("sg_hc_heatmap", new_otu_id)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "HierarchicalClusteringHeatmap分析结果输出目录"],
            ["./out_otu.xls", "xls", "HierarchicalClusteringHeatmap分析可视化结果数据表"]
        ])
        super(HierarchicalClusteringHeatmapWorkflow, self).end()

    def run(self):
        self.get_species()
        self.run_sort_samples()
        super(HierarchicalClusteringHeatmapWorkflow, self).run()
