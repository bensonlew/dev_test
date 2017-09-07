# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'

from biocluster.workflow import Workflow
from biocluster.config import Config
import glob
import os
from bson.objectid import ObjectId


class GenesetEnrichWorkflow(Workflow):
    """
    基因集富集分析
    """
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        self.rpc = False
        super(GenesetEnrichWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "kegg_table", "type": "string"},
            {"name": "go_list", "type": "string"},
            {"name": "genset_list", "type": "string"},
            {"name": "all_list", "type": "string"},
            {"name": "anno_type", "type": "string"},
            {"name": "geneset_type", "type": "string"},
            {"name": "update_info", "type": "string"},
            {"name": "main_table_id", "type": "string"},
            {"name": "submit_location", "type": "string"},
            {"name": "task_type", "type": "string"},
            {"name": "method", "type": "string"},
            {"name": "add_info", "type": "string", "default": None},  # 输入两列的列表文件，有head，第一列为pathway，第二列为底图链接
            {"name": "class_code_info", "type": "string"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.enrich_tool = self.add_tool("denovo_rna.express.go_enrich") if self.option("anno_type") == "go" else self.add_tool("denovo_rna.express.kegg_rich")
        self.output_dir = self.enrich_tool.output_dir
        # self.group_spname = dict()

    def run(self):
        background_path = self.get_background_gene()
        if self.option("anno_type") == "kegg":
            options = {
                "kegg_table": self.option("kegg_table"),
                # "all_list": background_path,
                "diff_list": self.option("genset_list"),
                "correct": self.option("method"),
                "add_info": self.option("add_info"),
                "class_code_info": self.option("class_code_info")
            }
        else:
            options = {
                "diff_list": self.option("genset_list"),
                # "all_list": background_path,
                "go_list": self.option("go_list"),
                # "pval": self.option("pval"),
                "method": self.option("method"),
                "class_code_info": self.option("class_code_info")
            }
        self.logger.info(options)
        self.enrich_tool.set_options(options)
        self.enrich_tool.on('end', self.set_db)
        self.enrich_tool.run()
        super(GenesetEnrichWorkflow, self).run()

    def get_background_gene(self):
        background_path = self.work_dir + "/background_gene"
        new_genes = set()
        ref_genes = set()
        geneset = set()
        with open(self.option("genset_list"), "r") as a:
            for line in a:
                line = line.strip().split("\t")
                geneset.add(line[0])
        with open(self.option("all_list"), "r") as g:
            for line in g:
                line = line.strip().split("\t")
                if line[-1] == "new":
                    new_genes.add(line[0])
                else:
                    ref_genes.add(line[0])
        self.logger.info(new_genes & geneset)
        self.logger.info(len(new_genes & ref_genes))
        if len(new_genes & geneset) == 0:
            with open(background_path, "w") as b:
                for rg in ref_genes:
                    b.write("{}\n".format(rg))
        else:
            print "lllllllllllsssssss"
            with open(background_path, "w") as b:
                for rg in (ref_genes | new_genes):
                    b.write("{}\n".format(rg))
        return background_path

    def set_db(self):
        """
        保存结果指数表到mongo数据库中
        """
        api_geneset = self.api.ref_rna_geneset
        output_file = glob.glob("{}/*.xls".format(self.output_dir))
        # png_file = glob.glob("{}/*.png".format(self.output_dir))
        # go_png = self.output_dir + "/go_lineage.png"
        # go_pdf = self.output_dir + "/go_lineage.pdf"
        go_adjust_png = self.output_dir + "/adjust_lineage.png"
        go_adjust_pdf = self.output_dir + "/adjust_lineage.pdf"
        genset_list_path = self.option("genset_list")
        all_list_path = self.option("all_list")
        if self.option("anno_type") == "kegg":
            api_geneset.add_kegg_enrich_detail(self.option("main_table_id"), output_file[0])
        else:
            api_geneset.add_go_enrich_detail(self.option("main_table_id"), output_file[0])
            # if len(png_file) == 1:
            api_geneset.update_directed_graph(self.option("main_table_id"), go_adjust_png, go_adjust_pdf)
        self.end()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        if self.option("anno_type") == "go":
            result_dir.add_relpath_rules([
                [".", "", "基因集GO富集分析结果文件"],
            ])
        elif self.option("anno_type") == "kegg":
            result_dir.add_relpath_rules([
                [".", "", "基因集KEGG富集分析结果文件"],
            ])
        super(GenesetEnrichWorkflow, self).end()

