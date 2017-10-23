# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'
# last_modify:2017.09.04


import os
import pandas as pd
import shutil
from biocluster.module import Module
from biocluster.core.exceptions import OptionError
from mainapp.models.mongo.public.meta.meta import Meta


class HeatmapModule(Module):
    """
    群落heatmap
    """
    def __init__(self, work_id):
        super(HeatmapModule, self).__init__(work_id)
        options = [
            {"name": "abundtable", "type": "infile", "format": "meta.otu.otu_table"},  # 物种/功能丰度表格
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "group_detail", "type": "string", "default": ""},  # 输入的group_detail,来自于网页选择
            {"name": "species_number", "type": "string", "default": "50"},  # 物种数目，默认top50
            {"name": "method", "type": "string", "default": ""},  # 物种层次聚类方式，默认不聚类
            {"name": "sample_method", "type": "string", "default": ""},  # 样本层次聚类方式，默认不聚类
            {"name": "add_Algorithm", "type": "string", "default": ""},  # 分组样本求和算法，默认不求和
        ]
        self.add_option(options)
        if self.option("group_detail") != "":
            group_table_path = os.path.join(self.work_dir, "group_table.xls")
            self.group_table_path = Meta().group_detail_to_table(self.option("group_detail"), group_table_path)

    def check_options(self):
        if not self.option("abundtable").is_set:
            raise OptionError("请传入物种/功能/基因丰度表格！")
        if not self.option("group").is_set and self.option("group_detail") == "":
            raise OptionError("请输入分组信息!")
        if self.option('method') not in ['average', 'single', 'complete', ""]:
            raise OptionError('错误的物种层次聚类方式：%s' % self.option('method'))
        if self.option('sample_method') not in ['average', 'single', 'complete', ""]:
            raise OptionError('错误的样本层次聚类方式：%s' % self.option('sample_method'))
        if self.option('add_Algorithm') not in ['sum', 'average', 'middle', ""]:
            raise OptionError('错误的样本求和方式：%s' % self.option('add_Algorithm'))
        if self.option("method") != "":
            if self.option("species_number") != "0":
                if int(self.option("species_number")) == 1:
                    raise OptionError('物种聚类的个数不能为：%s' % self.option('species_number'))

    def set_resource(self):
        self._cpu = 2
        self._memory = '3G'

    def get_species(self):
        global new_otu_file_path
        old_otu_file_path = self.option("abundtable").prop["path"]
        if self.option("sample_method") != "":
            if self.option("group").is_set:
                sample_n = open(self.option("group").prop["path"], "r")
            else:
                print(self.group_table_path)
                sample_n = open(self.group_table_path.prop["path"], "r")
            content = sample_n.readlines()
            if len(content) == 2:
                raise OptionError('样本聚类的个数不能为：1' )
            if self.option("add_Algorithm") != "":
                sample_ = []
                if self.option("group").is_set:
                    sample_n2 = open(self.option("group").prop["path"], "r")
                else:
                    sample_n2 = open(self.group_table_path.prop["path"], "r")
                content = sample_n2.readlines()
                for f in content:
                    f = f.strip("\n")
                    arr = f.strip().split("\t")
                    if arr[0] != "#sample":
                        if arr[1] not in sample_:
                            sample_.append(arr[1])
                if len(sample_) == 1:
                    raise OptionError('当计算分组丰度并且进行样本聚类的分析时,样本分组不能为1' )

        df = pd.DataFrame(pd.read_table(old_otu_file_path, sep='\t', index_col=0))
        df['Col_sum'] = df.apply(lambda x: x.sum(), axis=1)
        number = int(self.option("species_number"))
        new_otu_file = df.sort_values(by=['Col_sum'], ascending=0).head(number)
        del new_otu_file["Col_sum"]
        new_otu_file_path = os.path.join(self.work_dir, "new_abund_table.xls")
        new_otu_file.to_csv(new_otu_file_path, sep="\t")

    def run_sort_samples(self):
        self.sort_samples = self.add_tool("meta.otu.sort_samples")
        if self.option("group").is_set:
            self.group_table_path = self.option("group")
        self.sort_samples.set_options({
            "in_otu_table": new_otu_file_path,
            "group_table": self.group_table_path,
            "method": self.option("add_Algorithm")
        })
        if self.option("sample_method") != "":
            self.sort_samples.on("end", self.run_matrix)
        elif self.option("sample_method") == "" and self.option("method") != "":
            self.sort_samples.on("end", self.run_species_matrix)
        elif self.option("method") == "" and self.option("method") == "":
            self.sort_samples.on("end", self.end)
        self.sort_samples.run()

    def run_matrix(self):
        """
        运行计算距离矩阵
        :return:
        """
        self.logger.info("正在进行样本距离计算")
        self.matrix = self.add_tool("meta.beta_diversity.distance_calc")
        self.matrix.set_options({'method': "bray_curtis",
                                'otutable': self.sort_samples.option("out_otu_table")})
        self.matrix.on('end', self.run_hcluster)
        self.matrix.run()

    def run_hcluster(self):
        self.hcluster = self.add_tool('meta.beta_diversity.hcluster')
        self.hcluster.set_options({
            'dis_matrix': self.matrix.option('dis_matrix'),
            'linkage': self.option("sample_method"),
        })
        if self.option("method") == "":
            self.hcluster.on('end', self.end)
        else:
            self.hcluster.on('end', self.run_species_matrix)
        self.hcluster.run()

    def run_species_matrix(self):
        self.logger.info("正在进行物种/功能/基因距离计算")
        trans_otu = os.path.join(self.work_dir, "otu.trans")
        self.sort_samples.option("out_otu_table").transposition(trans_otu)
        self.species_matrix = self.add_tool("meta.beta_diversity.distance_calc")
        self.species_matrix.set_options({
            "method": "bray_curtis",
            "otutable": trans_otu
        })
        self.species_matrix.on('end', self.run_species_cluster)
        self.species_matrix.run()

    def run_species_cluster(self):
        self.species_hcluster = self.add_tool('meta.beta_diversity.hcluster')
        self.species_hcluster.set_options({
            'dis_matrix': self.species_matrix.option('dis_matrix'),
            'linkage': self.option("method")
        })
        self.species_hcluster.on('end', self.end)
        self.species_hcluster.run()

    def run(self):
        super(HeatmapModule, self).run()
        self.get_species()
        self.run_sort_samples()

    def end(self):
        shutil.copy(self.sort_samples.output_dir + "/taxa.table.xls", self.output_dir + "/taxa.table.xls")
        shutil.copy(self.sort_samples.output_dir + "/taxa.percents.table.xls",
                    self.output_dir + "/taxa.percents.table.xls")
        if self.option("sample_method") != "":
            shutil.copy(self.hcluster.output_dir + "/hcluster.tre", self.output_dir + "/specimen_hcluster.tre")
        if self.option("method") != "":
            shutil.copy(self.species_hcluster.output_dir+"/hcluster.tre", self.output_dir + "/species_hcluster.tre")
        super(HeatmapModule, self).end()
