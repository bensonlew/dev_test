# -*- coding: utf-8 -*-
# __author__ = 'zhujuan'

"""群落组成分析workflow"""
import os
import re
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.workflow import Workflow


class CompositionWorkflow(Workflow):
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(CompositionWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "update_info", "type": "string"},
            {"name": "anno_type", "type": "string"},
            {"name": "geneset_id", "type": "string"},
            {"name": "anno_id", "type": "string"},
            {"name": "level_id", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "params", "type": "string"},
            {"name": "anno_table", "type": "infile", "format": "meta.profile"},  
            {"name": "geneset_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "gene_list", "type": "infile", "format": "meta.profile"},
            #{"name": "level_type", "type": "string", "default": ""},
            {"name": "level_type_name", "type": "string", "default": ""},
            {"name": "lowest_level", "type": "string", "default": ""},  
            {"name": "graphic_type", "type": "string", "default": "bar"},  # bar,heatmap,circos
            {"name": "abund_file", "type": "infile", "format": "meta.otu.otu_table"}, 
            {"name": "group_detail", "type": "string"},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "others", "type": "float", "default": 0.01}, 
            {"name": "species_number", "type": "string", "default": "50"},
            {"name": "method", "type": "string"},
            {"name": "species_method", "type": "string", "default": ""},
            {"name": "sample_method", "type": "string", "default": ""},
            {"name": "group_method", "type": "string", "default": ""},
            {"name": "main_id", "type": "string"},
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.composition_analysis = self.add_module("meta.composition.composition_analysis")

    def run_get_abund_table(self):
        self.get_abund_table = self.add_tool("meta.create_abund_table")
        self.get_abund_table.set_options({
            'anno_table': self.option('anno_table'),
            'geneset_table': self.option('geneset_table'),
            'gene_list': self.option('gene_list'),
            'level_type': self.option('level_id'),
            'level_type_name': self.option('level_type_name'),
            'lowest_level': self.option('lowest_level'),
        })
        self.get_abund_table.on("end", self.run_composition_analysis)
        self.get_abund_table.run()

    def run_composition_analysis(self):
        if self.option("abund_file").is_set:
            abund_table = self.option("abund_file")
        else:
            abund_table = self.get_abund_table.option("out_table").prop['path']
        self.logger.info(abund_table)
        self.composition_analysis.set_options({
            "analysis": self.option('graphic_type'),
            "abundtable": abund_table,
            "group": self.option('group'),
            "add_Algorithm":  self.option('group_method'),
            "method":  self.option('species_method'),
            "sample_method":  self.option('sample_method'),
            "species_number":  self.option('species_number'),
            "others":  self.option('others'),
        })
        self.composition_analysis.on("end", self.set_db)
        self.composition_analysis.run()

    def set_db(self):
        self.logger.info("正在写入mongo数据库")
        api_composition = self.api.composition
        if self.option("graphic_type") in ["heatmap"]:
            specimen_tree = ""
            species_tree = ""
            if self.option("species_method") != "":
                species_tree = self.composition_analysis.output_dir.rstrip('/') + '/heatmap/species_hcluster.tre'
            if self.option("sample_method") != "":
                specimen_tree = self.composition_analysis.output_dir.rstrip('/') + '/heatmap/specimen_hcluster.tre'
            file_path = self.composition_analysis.output_dir.rstrip('/') + '/heatmap/taxa.percents.table.xls'
            api_composition.add_composition_detail(file_path, self.option("main_id"), species_tree=species_tree,
                                                   specimen_tree=specimen_tree)
        elif self.option("graphic_type") in ["bar"]:
            file_path = self.composition_analysis.output_dir.rstrip('/') + '/bar/taxa.percents.table.xls'
            api_composition.add_composition_detail(file_path, self.option("main_id"), species_tree="",
                                                   specimen_tree="")
        elif self.option("graphic_type") in ["circos"]:
            file_path = self.composition_analysis.output_dir.rstrip('/') + '/circos/taxa.percents.table.xls'
            api_composition.add_composition_detail(file_path, self.option("main_id"), species_tree="",
                                                   specimen_tree="")
        self.end()

    def end(self): 
        if self.option("graphic_type") in ["heatmap"]:
            shutil.copy(self.composition_analysis.output_dir + "heatmap/taxa.percents.table.xls", self.output_dir + "/heatmap.taxa.table.xls")
            if os.path.exists(self.composition_analysis.output_dir + "/heatmap/specimen_hcluster.tre"):
                shutil.copy(self.composition_analysisr.output_dir + "/heatmap/specimen_hcluster.tre", self.output_dir + "/specimen_hcluster.tre")
            if os.path.exists(self.composition_analysis.output_dir + "/heatmap/species_hcluster.tre"):
                shutil.copy(self.composition_analysis.output_dir + "/heatmap/species_hcluster.tre", self.output_dir + "/species_hcluster.tre")
            result_dir = self.add_upload_dir(self.output_dir)
            result_dir.add_relpath_rules([
                [".", "", "群落Heatmap分析结果输出目录"],
                ["./heatmap.taxa.table.xls", "xls", "群落Heatmap分析可视化结果数据表"],
                ["./specimen_hcluster.tre", "tre", "样本聚类树"],
                ["./species_hcluster.tre", "tre", "物种聚类树"]
            ])
        elif self.option("graphic_type") in ["bar"]:
            shutil.copy(self.composition_analysis.output_dir + "/bar/taxa.table.xls", self.output_dir + "/taxa.table.xls")
            shutil.copy(self.composition_analysis.output_dir + "/bar/taxa.percents.table.xls", self.output_dir + "/taxa.percents.table.xls")
            result_dir = self.add_upload_dir(self.output_dir)
            result_dir.add_relpath_rules([
                [".", "", "物种组成分析bar结果目录"],
                ["taxa.table.xls", "xls", "各样本物种丰度结果表"],  
                ["taxa.percents.table.xls", "xls", "各样本物种相对丰度结果表"]  
            ])
        elif self.option("graphic_type") in ["circos"]:
            shutil.copy(self.composition_analysis.output_dir + "/circos/taxa.table.xls", self.output_dir + "/taxa.table.xls")
            shutil.copy(self.composition_analysis.output_dir + "/circos/taxa.percents.table.xls", self.output_dir + "/taxa.percents.table.xls")
            result_dir = self.add_upload_dir(self.output_dir)
            result_dir.add_relpath_rules([
                [".", "", "物种组成分析circos结果目录"],
                ["taxa.table.xls", "xls", "各样本物种丰度结果表"],
                ["taxa.percents.table.xls", "xls", "各样本物种相对丰度结果表"]
            ])
        super(CompositionWorkflow, self).end()

    def run(self):
        self.IMPORT_REPORT_DATA = True
        self.IMPORT_REPORT_DATA_AFTER_END = False
        if self.option("abund_file").is_set:
            self.run_composition_analysis()
        else:
            self.run_get_abund_table()
        super(CompositionWorkflow, self).run()
