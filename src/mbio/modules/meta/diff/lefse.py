# -*- coding: utf-8 -*-
# __author__ = 'gaohao'

import os
from biocluster.module import Module
from biocluster.core.exceptions import OptionError


class LefseModule(Module):
    """"
    lefse分析
    """
    def __init__(self,work_id):
        super(LefseModule,self).__init__(work_id)
        options = [
            {"name": "anno_table", "type": "infile", "format": "meta.profile"},  # 各数据库的注释表格
            {"name": "geneset_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "gene_list", "type": "infile", "format": "meta.profile"},
            {"name": "level_type", "type": "string", "default": ""},
            # 注释表的字段，eg：Pathway，Level1，Level2
            {"name": "level_type_name", "type": "string", "default": ""},
            # 注释表字段的具体levelname，eg：Level1下Metabolism(对应的KO)
            {"name": "lowest_level", "type": "string", "default": ""},  # 注释表数据库对应的最低分类，eg：KEGG的ko
            {"name": "lefse_input", "type": "infile", "format": "meta.otu.otu_table"},  # 输入文件，biom格式的otu表
            {"name": "lefse_group", "type": "infile", "format": "meta.otu.group_table"},  # 输入分组文件
            {"name": "lda_filter", "type": "float", "default": 2.0},
            {"name": "lefse_type", "type": "string", "default": "meta_taxon"}, ###meta_taxon,metagenome_taxon,metagenome_func
            {"name": "strict", "type": "int", "default": 0},
            {"name": "lefse_gname", "type": "string"},
            {"name": "start_level", "type": "int", "default": 3},
            {"name": "end_level", "type": "int", "default": 7}
        ]
        self.add_option(options)
        self.func_abundance_table = self.add_tool("meta.create_abund_table")
        self.lefse = self.add_tool('statistical.lefse')
        self.taxon_abundance_table  = self.add_tool('meta.create_mg_taxon_abund_table')

    def check_options(self):  ##物种分类
        if not self.option("geneset_table").is_set:
            raise OptionError("请输入物种/功能/基因丰度表格！")
        if not self.option("anno_table").is_set:
            raise OptionError("请输入功能注释表格！")
        if not self.option("lefse_group").is_set:
            raise OptionError("请输入分组信息!")
        if self.option('lefse_type') not in ["meta_taxon","metagenome_taxon","metagenome_func"]:
            raise OptionError("请输入分析lefse的类型")
        


    def set_resource(self):
        self._cpu = 2
        self._memory = '3G'


    def run_func_abundance_table(self):
        """
        计算metagenome的功能丰度表
        :return:
        """
        self.logger.info("正在计算丰度表")
        self.func_abundance_table.set_options({
          "geneset_table": self.option("geneset_table"),
          "anno_table":self.option("anno_table"),
          "level_type":self.option("level_type")
       })
        self.func_abundance_table.on('end', self.run_lefse)
        self.func_abundance_table.run()

    def run_taxon_abundance_table(self):
        """
        计算metagenome丰度表
        :return:
        """
        self.logger.info("正在计算丰度表")
        self.taxon_abundance_table.set_options({
          "geneset_table": self.option("geneset_table"),
          "anno_table":self.option("anno_table")
       })
        self.taxon_abundance_table.on('end', self.run_lefse)
        self.taxon_abundance_table.run()

    def run_lefse(self):

        self.logger.info("正在进行lefse分析")
        if not self.option('lefse_input').is_set:
            if (self.option('lefse_type') == 'metagenome_taxon'):
                self.lefse.set_options({
                   "lefse_input": self.taxon_abundance_table.option("out_table"),
                   "lefse_group": self.option("lefse_group"),
                   "lda_filter": self.option("lda_filter"),
                   "strict": self.option("strict"),
                   "lefse_gname": self.option("lefse_gname"),
                   "start_level": self.option("start_level"),
                    "end_level": self.option("end_level")
                })
            elif():
                self.lefse.set_options({
                    "lefse_input": self.func_abundance_table.option("out_table"),
                    "lefse_group": self.option("lefse_group"),
                    "lda_filter": self.option("lda_filter"),
                    "strict": self.option("strict"),
                    "lefse_gname": self.option("lefse_gname"),
                    "start_level": self.option("start_level"),
                    "end_level": self.option("end_level")
                })
        else:
            self.lefse.set_options({
                "lefse_input": self.option("lefse_input"),
                "lefse_group": self.option("lefse_group"),
                "lda_filter": self.option("lda_filter"),
                "strict": self.option("strict"),
                "lefse_gname": self.option("lefse_gname"),
                "start_level": self.option("start_level"),
                "end_level": self.option("end_level"),

            })

        self.lefse.on('end',self.end)
        self.lefse.run()

    def run(self):
        super(LefseModule,self).run()
        if (self.option('lefse_type') == 'meta_taxon'):
            self.run_lefse()
        elif (self.option('lefse_type') == 'metagenome_taxon'):
            self.run_taxon_abundance_table()
        elif (self.option('lefse_type') == 'metagenome_func'):
            self.run_func_abundance_table()


    def end(self):
        super(LefseModule, self).end()
